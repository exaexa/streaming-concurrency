{- |
   Module      : Streaming.Concurrent.Deterministic
   Description : Some additional deterministic concurrent stream processors
   Copyright   : Mirek Kratochvil
   License     : MIT
   Maintainer  : exa.exa@gmail.com

   This adds several deterministic counterparts to these in module
   "Streaming.Concurrent".

 -}
module Streaming.Concurrent.Deterministic where

import Control.Concurrent
import Control.Concurrent.STM.TChan
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.STM
import qualified Streaming as S
import Streaming (Of(..), Sum(..))
import Streaming.Internal (Stream(..))

-- | Helper for forcing a stream towards a `Step` and putting the result into a
-- `MVar`.
--
-- TODO: this should catch errors.
stepIntoMVar :: MonadIO m => MVar (Stream f m r) -> Stream f m r -> m ()
stepIntoMVar mv (Effect m) = m >>= stepIntoMVar mv
stepIntoMVar mv x = liftIO (putMVar mv x)

-- | Given a traversable container of streams that run in IO, make a stream of
-- traversables where the elements in streams are evaluated concurrently by
-- batches.
--
-- Intuitively, this implements a Traversable-generic `zip` where inputs are
-- retrieved concurrently.
sequenceForkIO ::
     (Traversable t, MonadIO m)
  => t (Stream (Of a) IO r)
  -> Stream (Of (t a)) m ()
sequenceForkIO streams0 = liftIO (traverse start streams0) >>= go
  where
    start s = do
      mv <- newEmptyMVar
      _ <- forkIO $ stepIntoMVar mv s
      pure mv
    go streams = do
      steps <- liftIO $ traverse takeMVar streams
      case traverse collect steps of
        Nothing -> Return ()
        Just ofs
          | null ofs -> Return ()
          | otherwise ->
            Step
              (fmap (\(a :> _) -> a) ofs
                 :> sequenceForkIO (fmap (\(_ :> b) -> b) ofs))
    collect (Step (a :> rest)) = Just (a :> rest)
    collect (Effect _) = error "sequenceForkIO: stepped on effect"
    collect (Return _) = Nothing

-- | Given a list of streams, produce a round-robin mixture of the stream
-- items. If the streams have `Effect`s, the effect evaluation is started in
-- the background so that there's a chance to have a `Step` ready in the next
-- robin round.
--
-- All streams are drained and the final stream values are returned in a list
-- in the same order as on input.
roundRobinForkIO :: MonadIO m => [Stream (Of a) IO r] -> Stream (Of a) m [r]
roundRobinForkIO streams = liftIO (traverse newMVar streams) >>= go 0 []
  where
    go n refills []
      | n >= length refills = traverse (liftIO . collect) $ reverse refills
      | otherwise = go n [] (reverse refills)
    go n refills (qvar:qvars) = do
      s <- liftIO $ takeMVar qvar
      let forkOff rest =
            liftIO (forkIO $ stepIntoMVar qvar rest)
              >> go 0 (qvar : refills) qvars
      case s of
        Step (a :> rest) -> Step (a :> forkOff rest)
        Effect _ -> forkOff s
        Return _ ->
          liftIO (putMVar qvar s) >> go (succ n) (qvar : refills) qvars
    collect =
      readMVar
        >=> S.mapsM_ (error "forkIORoundRobin: drained stream contained Step")

-- | Like `S.mapM` but the next `n0` evaluations of `f` are forked off and
-- evaluated concurrently.
mapMForkNIO ::
     MonadIO m => Int -> (a -> IO b) -> Stream (Of a) m r -> Stream (Of b) m r
mapMForkNIO n0 f = go 0 [] []
  where
    -- if we can run an effect, do it
    go n qs rs (Effect m) = Effect $ go n qs rs <$> m
    -- and start as many threads as possible soon
    go n qs rs (Step (a :> rest))
      | n < n0 = do
        mv <- liftIO newEmptyMVar
        _ <- liftIO $ forkIO (f a >>= putMVar mv) -- TODO catch errors.
        go (succ n) qs (mv : rs) rest
    -- if all queues are empty and there's nothing left, finish
    go _ [] [] (Return r) = Return r
    -- if we emptied both queues and the stuff didn't effect or refill,
    -- there's a consistency issue with the counter
    go _ [] [] _ = error "mapMForkNIO: counting issue"
    -- refill the forward queue if empty
    go n [] rs s = go n (reverse rs) [] s
    -- finally if the forward queue is not empty,
    -- wait for and yield one result
    go n (q:qrest) rs s = do
      b <- liftIO $ takeMVar q
      Step $ b :> go (pred n) qrest rs s

-- | Given a function that expands a seed into either more seeds, or multiple
-- final values, produce a stream of the final values. The order in lists is
-- preserved, i.e., the head seeds are unfolded first.
unfoldStream ::
     Monad m => (a -> m [Either a b]) -> [Either a b] -> Stream (Of b) m ()
unfoldStream gen = go
  where
    go [] = Return ()
    go (Right b:qs) = Step (b :> go qs)
    go (Left a:qs) =
      Effect $ do
        qs' <- gen a
        pure $ go (qs' ++ qs)

-- | Helper for internal evaluation queue in `unfoldStreamForkNIO`.
data InQueue a b
  = InQStarted !Int
  | InQReady b
  | InQFolded a
  deriving (Show)

-- | Convert an `unfoldStream`-like seed or result into an `InQueue` element.
toQueue :: Either a b -> InQueue a b
toQueue (Left a) = InQFolded a
toQueue (Right b) = InQReady b

-- | Like `unfoldStream`, but the first `maxthreads` seeds in the queue are
-- expanded in parallel. Since the expansion order is not deterministic and
-- depends on the order in which the operations finish, this may require to
-- cache some final values that are known evaluated but can't be streamed out
-- yet. To prevent memory exhaustion, the queue of outputs is limited by
-- `maxqueue`. Hitting the queue limit causes `unfoldStream` to stop spawning
-- off new evaluation threads, falling back to serial behavior of
-- `unfoldStream`.
--
-- Good size of queue depends on the shape of the unfolding.  As a rule of
-- thumb there should be
-- @O(maxthreads * expectedUnfoldDepth * (1+expectedNumOfOutputsGeneratedAtThatDepth))@
-- available places in the queue. For a binary-tree-shaped unfold that
-- generates leaves in depth 10 using 10 threads, one would thus recommend
-- `maxqueue` of at least 200.
unfoldStreamForkNIO ::
     MonadIO m
  => Int -- ^ maximum number of concurrent threads
  -> Int -- ^ maximum number of items in queue
  -> (a -> IO [Either a b]) -- ^ unfolding function
  -> [Either a b] -- ^ seed(s)
  -> Stream (Of b) m ()
unfoldStreamForkNIO maxthreads maxqueue gen seeds =
  S.lift (liftIO newTChanIO) >>= start 0 (map toQueue seeds)
  where
    -- drain whatever steps available and measure how much tasks we can spawn
    start _ [] _ = Return ()
    start i (InQReady b:qs) chan = Step (b :> start i qs chan)
    start i q chan =
      let (ns, nt) = scan 0 0 q
          capacity
            | nt > maxqueue = 0
            | otherwise = maxthreads - ns
          capacity'
            | capacity <= 0 && ns <= 0 = 1 -- unblock unfolding (queue full of folded stuff)
            | otherwise = capacity
       in do
            (i', q') <- S.lift $ spawn i capacity' q chan
            wait i' q' chan
    -- work over the queue and spawn @n@ tasks at the start
    spawn i n qs _
      | n <= 0 = pure (i, qs)
      | [] <- qs = pure (i, qs)
    spawn i n (InQFolded a:qs) chan = do
      -- TODO manage errors here:
      _ <- liftIO . forkIO $ gen a >>= atomically . writeTChan chan . (,) i
      fmap (InQStarted i :) <$> spawn (succ i) (pred n) qs chan
    spawn i n (x:qs) chan = fmap (x :) <$> spawn i n qs chan
    -- wait until something comes, save it to queue, and restart
    wait i q chan = do
      (ri, r) <- S.lift . liftIO . atomically $ readTChan chan
      let q' = save ri r q
      peek <- S.lift . liftIO . atomically $ tryPeekTChan chan
      case peek of
        Just _ -> wait i q' chan
        Nothing -> start i q' chan
    -- measure the queue and the amount of uncollected threads
    scan s t (InQStarted _:qs) = scan (succ s) (succ t) qs
    scan s t (_:qs) = scan s (succ t) qs -- total
    scan s t [] = (s, t)
    -- save a result of a started computation into the queue
    save _ _ [] = error "unfoldStreamForkNIO: placeholder disappeared?"
    save ri r (InQStarted ri':qs)
      | ri == ri' = map toQueue r ++ qs
    save ri r (q:qs) = q : save ri r qs
