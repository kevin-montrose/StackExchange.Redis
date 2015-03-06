using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using System.Threading;
using System.Collections.Concurrent;

namespace StackExchange.Redis.Tests
{
    [TestFixture]
    public class Profiling : TestBase
    {
        class TestEventSink : IProfilerEventSink
        {
            ConcurrentBag<IProfiledCommand> Context;

            public TestEventSink(ConcurrentBag<IProfiledCommand> context)
            {
                Context = context;
            }

            public void FinishProfiling(IProfiledCommand command)
            {
                Context.Add(command);
            }
        }

        class TestProfiler : IProfiler
        {
            public ThreadLocal<ConcurrentBag<IProfiledCommand>> Context = new ThreadLocal<ConcurrentBag<IProfiledCommand>>(() => new ConcurrentBag<IProfiledCommand>(), trackAllValues: true);

            public IProfilerEventSink BeginProfiling()
            {
                return new TestEventSink(Context.Value);
            }
        }

        [Test]
        public void Simple()
        {
            using (var conn = Create())
            {
                var profiler = new TestProfiler();

                conn.RegisterProfiler(profiler);
                var db = conn.GetDatabase(4);
                db.StringSet("hello", "world");
                var val = db.StringGet("hello");
                Assert.AreEqual("world", (string)val);

                var cmds = profiler.Context.Value;
                Assert.AreEqual(2, cmds.Count);

                var set = cmds.SingleOrDefault(cmd => cmd.Command == "SET");
                Assert.IsNotNull(set);
                var get = cmds.SingleOrDefault(cmd => cmd.Command == "GET");
                Assert.IsNotNull(get);

                Assert.IsTrue(set.CommandCreated <= get.CommandCreated);

                Assert.AreEqual(4, set.Db);
                Assert.AreEqual(conn.GetEndPoints()[0], set.EndPoint);
                Assert.IsTrue(set.CreationToEnqueued > TimeSpan.Zero);
                Assert.IsTrue(set.EnqueuedToSending > TimeSpan.Zero);
                Assert.IsTrue(set.SentToResponse > TimeSpan.Zero);
                Assert.IsTrue(set.ElapsedTime > TimeSpan.Zero);
                Assert.IsTrue(set.ElapsedTime > set.CreationToEnqueued && set.ElapsedTime > set.EnqueuedToSending && set.ElapsedTime > set.SentToResponse);

                Assert.AreEqual(4, get.Db);
                Assert.AreEqual(conn.GetEndPoints()[0], get.EndPoint);
                Assert.IsTrue(get.CreationToEnqueued > TimeSpan.Zero);
                Assert.IsTrue(get.EnqueuedToSending > TimeSpan.Zero);
                Assert.IsTrue(get.SentToResponse > TimeSpan.Zero);
                Assert.IsTrue(get.ElapsedTime > TimeSpan.Zero);
                Assert.IsTrue(get.ElapsedTime > get.CreationToEnqueued && get.ElapsedTime > get.EnqueuedToSending && get.ElapsedTime > get.SentToResponse);
            }
        }

        [Test]
        public void ManyThreads()
        {
            using (var conn = Create())
            {
                var profiler = new TestProfiler();

                conn.RegisterProfiler(profiler);

                var threads = new List<Thread>();

                for (var i = 0; i < 16; i++)
                {
                    var db = conn.GetDatabase(i);

                    threads.Add(
                        new Thread(
                            delegate()
                            {
                                var threadTasks = new List<Task>();

                                for (var j = 0; j < 1000; j++)
                                {
                                    var task = db.StringSetAsync("" + j, "" + j);
                                    threadTasks.Add(task);
                                }

                                Task.WaitAll(threadTasks.ToArray());
                            }
                        )
                    );
                }

                threads.ForEach(thread => thread.Start());
                threads.ForEach(thread => thread.Join());

                var allVals = profiler.Context.Values;

                var kinds = allVals.SelectMany(bucket => bucket.Select(cmd => cmd.Command)).Distinct().ToList();
                Assert.IsTrue(kinds.Count <= 2);
                Assert.IsTrue(kinds.Contains("SET"));
                if (kinds.Count == 2 && !kinds.Contains("SELECT"))
                {
                    Assert.Fail("Non-SET, Non-SELECT command seen");
                }

                Assert.AreEqual(16, allVals.Count);
                Assert.AreEqual(16 * 1000, allVals.Sum(bucket => bucket.Count(cmd => cmd.Command == "SET")));

                foreach (var bucket in allVals)
                {
                    Assert.AreEqual(1000, bucket.Count(cmd => cmd.Command == "SET"));
                    var firstDb = bucket.First().Db;
                    Assert.IsTrue(bucket.All(cmd => cmd.Db == firstDb));
                }
            }
        }
    }
}
