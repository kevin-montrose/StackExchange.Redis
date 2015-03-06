using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace StackExchange.Redis
{
    /// <summary>
    /// A profile command against a redis instance.
    /// </summary>
    public interface IProfiledCommand
    {
        /// <summary>
        /// The endpoint this command was sent to.
        /// </summary>
        EndPoint EndPoint { get; }

        /// <summary>
        /// The Db this command was sent to.
        /// </summary>
        int Db { get; }

        /// <summary>
        /// The name of this command.
        /// </summary>
        string Command { get; }

        /// <summary>
        /// When this command was *created*, will be approximately
        /// when the paired method of StackExchange.Redis was called but
        /// before that method returned.
        /// </summary>
        DateTime CommandCreated { get; }

        /// <summary>
        /// How long this command waited to be added to the queue of pending
        /// redis commands.  A large TimeSpan indicates serious contention for
        /// the pending queue.
        /// </summary>
        TimeSpan CreationToEnqueued { get; }
        
        /// <summary>
        /// How long this command spent in the pending queue before being sent to redis.
        /// A large TimeSpan can indicate a large number of pending events, large pending events,
        /// or network issues.
        /// </summary>
        TimeSpan EnqueuedToSending { get; }
        
        /// <summary>
        /// How long before Redis responded to this command and it's response could be handled after it was sent.
        /// A large TimeSpan can indicate a large response body, an overtaxed redis instance, or network issues.
        /// </summary>
        TimeSpan SentToResponse { get; }

        /// <summary>
        /// How long it took this redis command to be processed, from creation to deserializing the final resposne.
        /// 
        /// Note that this TimeSpan *does not* include time spent awaiting a Task in consumer code.
        /// </summary>
        TimeSpan ElapsedTime { get; }
    }

    /// <summary>
    /// In order to receive callbacks with profiling data, consumers must return an implementation
    /// of this interface from IProfiler.BeginProfiling.
    /// 
    /// This class exists to allow context to be captured from the thread which *calls into* StackExchange.Redis.
    /// 
    /// Subsequent callers of methods on this interface can come from any thread.
    /// </summary>
    public interface IProfilerEventSink
    {
        /// <summary>
        /// Called when a command sent to redis has completed.  Full timing information is available on 
        /// the passed in IProfiledCommand object.
        /// 
        /// Note that this method can be called from any method, do not assume that the same thread
        /// which called IProfiler.BeginProfiling will call IProfilerEventSink.FinishProfiling.
        /// </summary>
        void FinishProfiling(IProfiledCommand command);
    }

    /// <summary>
    /// Interface for profiling individual commands against an Redis ConnectionMulitplexer.
    /// </summary>
    public interface IProfiler
    {
        /// <summary>
        /// Called to create an IProfilerEventSink that will be notified when a command completes.
        /// 
        /// This method will always be called from the thread *calling into* StackExchange.Redis, callbacks on
        /// the returned IProfilerEventSink can be from any thread.  If you need to capture context (such as
        /// an HTTP request), capture it in your IProfilerEventSink.
        /// </summary>
        IProfilerEventSink BeginProfiling();
    }
}
