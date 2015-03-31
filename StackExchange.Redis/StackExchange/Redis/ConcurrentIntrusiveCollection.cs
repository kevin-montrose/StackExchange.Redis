using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace StackExchange.Redis
{
    /// <summary>
    /// A thread-safe collection tailored to the "always append, with high contention, then enumerate once with no contention"
    /// behavior of our profiling.
    /// 
    /// Performs better than ConcurrentBag, which is important since profiling code shouldn't impact timings.
    /// </summary>
    sealed class ConcurrentIntrusiveCollection<T>
        where T : IIntrusiveCollectionElement<T>
    {
        // internal for test purposes
        internal static int AllocationCount = 0;

        // It is, by definition, impossible for an element to be in 2 intrusive collections
        //   and we force Enumeration to release any reference to the collection object
        //   so we can **always** pool these (by type).
        const int PoolSize = 64;
        static ConcurrentIntrusiveCollection<T>[] Pool = new ConcurrentIntrusiveCollection<T>[PoolSize];

        volatile IIntrusiveCollectionElement<T> Head;

        private ConcurrentIntrusiveCollection() { }

        /// <summary>
        /// This method is thread-safe.
        /// 
        /// Adds an element to the bag.
        /// 
        /// Order is not preserved.
        /// 
        /// The element can only be a member of *one* bag.
        /// </summary>
        public void Add(T command)
        {
            do
            {
                var cur = Head;
                command.NextElement = cur;

                // Interlocked references to volatile fields are perfectly cromulent
#pragma warning disable 420
                var got = Interlocked.CompareExchange(ref Head, command, cur);
#pragma warning restore 420

                if (object.ReferenceEquals(got, cur)) break;
            } while (true);
        }

        // Seperate method to ensure that no *this* is captured with all these `yield`-shenanigans.
        // This is not technically necessary, given a close reading of the C# guarantees... but
        //   relying on everyone to remember that is perhaps a bit much.
        static IEnumerable<T> MakeEnumerable(IIntrusiveCollectionElement<T> head)
        {
            // This is implemented as a lazy enumerable
            //   so that there's only one, relatively small, allocation. 
            //   (example generated class can be found here: http://csharpindepth.com/articles/chapter6/iteratorblockimplementation.aspx )
            // Turning it into a List or array feels wasteful.
            var cur = head;
            while (cur != null)
            {
                yield return cur.Value;
                cur = cur.NextElement;
            }
        }

        /// <summary>
        /// This method returns an enumerable view of the bag.
        /// 
        /// It is not thread safe.
        /// 
        /// It should only be called once the bag is finished being mutated.
        /// </summary>
        public IEnumerable<T> EnumerateAndReturnForReuse()
        {
            var ret = MakeEnumerable(Head);

            // no need for interlocking, this isn't a thread safe method
            Head = null;

            for (var i = 0; i < PoolSize; i++)
            {
                if (Interlocked.CompareExchange(ref Pool[i], this, null) == null) break;
            }

            return ret;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public static ConcurrentIntrusiveCollection<T> GetOrCreate()
        {
            ConcurrentIntrusiveCollection<T> found;
            for (int i = 0; i < PoolSize; i++)
            {
                if ((found = Interlocked.Exchange(ref Pool[i], null)) != null)
                {
                    return found;
                }
            }

            Interlocked.Increment(ref AllocationCount);
            found = new ConcurrentIntrusiveCollection<T>();

            return found;
        }
    }

    /// <summary>
    /// To avoid allocations, ConcurrentAddOnlyBag stores references for the link list
    /// in the actual elements being linked together.
    /// 
    /// Implementing this interfaces allows an element to be stored in a ConcurrentIntrusiveCollection
    /// </summary>
    interface IIntrusiveCollectionElement<Self>
        where Self : IIntrusiveCollectionElement<Self>
    {
        /// <summary>
        /// Gets or sets the next element in the bag.
        /// </summary>
        IIntrusiveCollectionElement<Self> NextElement { get; set; }

        /// <summary>
        /// Typed reference to the actual value.
        /// </summary>
        Self Value { get; }
    }
}
