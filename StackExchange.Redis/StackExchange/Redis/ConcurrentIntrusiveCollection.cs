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
        volatile IIntrusiveCollectionElement<T> Head;

        public ConcurrentIntrusiveCollection() { }

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

                // Interlocked references to voliatle fields are perfectly cromulent
#pragma warning disable 420
                var got = Interlocked.CompareExchange(ref Head, command, cur);
#pragma warning restore 420

                if (object.ReferenceEquals(got, cur)) break;
            } while (true);
        }

        /// <summary>
        /// This method returns an enumerable view of the bag.
        /// 
        /// It is not thread safe.
        /// 
        /// It should only be called once the bag is finished being mutated.
        /// </summary>
        public IEnumerable<T> Enumerate()
        {
            // This is implemented as a lazy enumerable
            //   so that there's only one, relatively small, allocation. 
            //   (example generated class can be found here: http://csharpindepth.com/articles/chapter6/iteratorblockimplementation.aspx )
            // Turning it into a List or array feels wasteful.
            var cur = Head;
            while (cur != null)
            {
                yield return cur.Value;
                cur = cur.NextElement;
            }
        }
    }

    /// <summary>
    /// To avoid allocations, ConcurrentAddOnlyBag stores references for the link list
    /// in the actual elements being linked together.
    /// 
    /// Implementing this interfaces allows an element to be stored in a ConcurrentAddOnlyBag
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
