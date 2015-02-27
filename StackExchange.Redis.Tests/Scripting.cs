using System;
using System.Diagnostics;
using NUnit.Framework;
using System.Linq;
using System.IO;

namespace StackExchange.Redis.Tests
{
    [TestFixture]
    public class Scripting : TestBase
    {
        [Test]
        public void TestBasicScripting()
        {
            using (var conn = Create())
            {
                RedisValue newId = Guid.NewGuid().ToString();
                RedisKey custKey = Me();
                var db = conn.GetDatabase();
                db.KeyDelete(custKey);
                db.HashSet(custKey, "id", 123);

                var wasSet = (bool) db.ScriptEvaluate(@"if redis.call('hexists', KEYS[1], 'UniqueId') then return redis.call('hset', KEYS[1], 'UniqueId', ARGV[1]) else return 0 end",
                    new RedisKey[] { custKey }, new RedisValue[] { newId });

                Assert.IsTrue(wasSet);

                wasSet = (bool)db.ScriptEvaluate(@"if redis.call('hexists', KEYS[1], 'UniqueId') then return redis.call('hset', KEYS[1], 'UniqueId', ARGV[1]) else return 0 end",
                    new RedisKey[] { custKey }, new RedisValue[] { newId });
                Assert.IsFalse(wasSet);
            }
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void CheckLoads(bool async)
        {
            using (var conn0 = Create(allowAdmin: true))
            using (var conn1 = Create(allowAdmin: true))
            {
                // note that these are on different connections (so we wouldn't expect
                // the flush to drop the local cache - assume it is a surprise!)
                var server = conn0.GetServer(PrimaryServer, PrimaryPort);
                var db = conn1.GetDatabase();
                const string script = "return 1;";

                // start empty
                server.ScriptFlush();
                Assert.IsFalse(server.ScriptExists(script));

                // run once, causes to be cached
                Assert.IsTrue((bool)db.ScriptEvaluate(script));
                Assert.IsTrue(server.ScriptExists(script));

                // can run again
                Assert.IsTrue((bool)db.ScriptEvaluate(script));

                // ditch the scripts; should no longer exist
                db.Ping();
                server.ScriptFlush();
                Assert.IsFalse(server.ScriptExists(script));
                db.Ping();

                if (async)
                {
                    // now: fails the first time
                    try
                    {
                        db.Wait(db.ScriptEvaluateAsync(script));
                        Assert.Fail();
                    }
                    catch(AggregateException ex)
                    {
                        Assert.AreEqual(1, ex.InnerExceptions.Count);
                        Assert.IsInstanceOf<RedisServerException>(ex.InnerExceptions[0]);
                        Assert.AreEqual("NOSCRIPT No matching script. Please use EVAL.", ex.InnerExceptions[0].Message);
                    }
                } else
                {
                    // just works; magic
                    Assert.IsTrue((bool)db.ScriptEvaluate(script));
                }

                // but gets marked as unloaded, so we can use it again...
                Assert.IsTrue((bool)db.ScriptEvaluate(script));

                // which will cause it to be cached
                Assert.IsTrue(server.ScriptExists(script));
            }
        }

        [Test]
        public void CompareScriptToDirect()
        {
            const string Script = "return redis.call('incr', KEYS[1])";

            using (var conn = Create(allowAdmin: true))
            {
                var server = conn.GetServer(PrimaryServer, PrimaryPort);
                server.FlushAllDatabases();
                server.ScriptFlush();

                server.ScriptLoad(Script);
                var db = conn.GetDatabase();
                db.Ping(); // k, we're all up to date now; clean db, minimal script cache

                // we're using a pipeline here, so send 1000 messages, but for timing: only care about the last
                const int LOOP = 5000;
                RedisKey key = "foo";
                RedisKey[] keys = new[] { key }; // script takes an array

                // run via script
                db.KeyDelete(key);
                CollectGarbage();
                var watch = Stopwatch.StartNew();
                for(int i = 1; i < LOOP; i++) // the i=1 is to do all-but-one
                {
                    db.ScriptEvaluate(Script, keys, flags: CommandFlags.FireAndForget);
                }
                var scriptResult = db.ScriptEvaluate(Script, keys); // last one we wait for (no F+F)
                watch.Stop();
                TimeSpan scriptTime = watch.Elapsed;

                // run via raw op
                db.KeyDelete(key);
                CollectGarbage();
                watch = Stopwatch.StartNew();
                for (int i = 1; i < LOOP; i++) // the i=1 is to do all-but-one
                {
                    db.StringIncrement(key, flags: CommandFlags.FireAndForget);
                }
                var directResult = db.StringIncrement(key); // last one we wait for (no F+F)
                watch.Stop();
                TimeSpan directTime = watch.Elapsed;

                Assert.AreEqual(LOOP, (long)scriptResult, "script result");
                Assert.AreEqual(LOOP, (long)directResult, "direct result");

                Console.WriteLine("script: {0}ms; direct: {1}ms",
                    scriptTime.TotalMilliseconds,
                    directTime.TotalMilliseconds);
            }
        }

        [Test]
        public void TestCallByHash()
        {
            const string Script = "return redis.call('incr', KEYS[1])";

            using (var conn = Create(allowAdmin: true))
            {
                var server = conn.GetServer(PrimaryServer, PrimaryPort);
                server.FlushAllDatabases();
                server.ScriptFlush();

                byte[] hash = server.ScriptLoad(Script);

                var db = conn.GetDatabase();
                RedisKey[] keys = { Me() };

                string hexHash = string.Concat(Array.ConvertAll(hash, x => x.ToString("X2")));
                Assert.AreEqual("2BAB3B661081DB58BD2341920E0BA7CF5DC77B25", hexHash);

                db.ScriptEvaluate(hexHash, keys);
                db.ScriptEvaluate(hash, keys);               

                var count = (int)db.StringGet(keys)[0];
                Assert.AreEqual(2, count);

            }
        }

        [Test]
        public void SimpleLuaScript()
        {
            const string Script = "return @ident";

            using(var conn = Create(allowAdmin: true))
            {
                var server = conn.GetServer(PrimaryServer, PrimaryPort);
                server.FlushAllDatabases();
                server.ScriptFlush();

                var prepared = LuaScript.Prepare(Script);

                var db = conn.GetDatabase();

                {
                    var val = prepared.Evaluate(db, new { ident = "hello" });
                    Assert.AreEqual("hello", (string)val);
                }

                {
                    var val = prepared.Evaluate(db, new { ident = 123 });
                    Assert.AreEqual(123, (int)val);
                }

                {
                    var val = prepared.Evaluate(db, new { ident = 123L });
                    Assert.AreEqual(123L, (long)val);
                }

                {
                    var val = prepared.Evaluate(db, new { ident = 1.1 });
                    Assert.AreEqual(1.1, (double)val);
                }

                {
                    var val = prepared.Evaluate(db, new { ident = true });
                    Assert.AreEqual(true, (bool)val);
                }

                {
                    var val = prepared.Evaluate(db, new { ident = new byte[] { 4, 5, 6 } });
                    Assert.IsTrue(new byte [] { 4, 5, 6}.SequenceEqual((byte[])val));
                }
            }
        }

        [Test]
        public void LuaScriptWithKeys()
        {
            const string Script = "redis.call('set', @key, @value)";

            using (var conn = Create(allowAdmin: true))
            {
                var server = conn.GetServer(PrimaryServer, PrimaryPort);
                server.FlushAllDatabases();
                server.ScriptFlush();

                var script = LuaScript.Prepare(Script);

                var db = conn.GetDatabase();

                var p = new { key = (RedisKey)"testkey", value = 123 };

                script.Evaluate(db, p);
                var val = db.StringGet("testkey");
                Assert.AreEqual(123, (int)val);

                // no super clean way to extract this; so just abuse InternalsVisibleTo
                RedisKey[] keys;
                RedisValue[] args;
                script.ExtractParameters(p, out keys, out args);
                Assert.IsNotNull(keys);
                Assert.AreEqual(1, keys.Length);
                Assert.AreEqual("testkey", (string)keys[0]);
            }
        }

        [Test]
        public void NoInlineReplacement()
        {
            const string Script = "redis.call('set', @key, 'hello@example')";
            using (var conn = Create(allowAdmin: true))
            {
                var server = conn.GetServer(PrimaryServer, PrimaryPort);
                server.FlushAllDatabases();
                server.ScriptFlush();

                var script = LuaScript.Prepare(Script);

                Assert.AreEqual("redis.call('set', ARGV[1], 'hello@example')", script.ExecutableScript);

                var db = conn.GetDatabase();

                var p = new { key = (RedisKey)"key" };

                script.Evaluate(db, p);
                var val = db.StringGet("key");
                Assert.AreEqual("hello@example", (string)val);
            }
        }

        [Test]
        public void EscapeReplacement()
        {
            const string Script = "redis.call('set', @key, @@escapeMe)";
            var script = LuaScript.Prepare(Script);

            Assert.AreEqual("redis.call('set', ARGV[1], @escapeMe)", script.ExecutableScript);
        }

        [Test]
        public void SimpleLoadedLuaScript()
        {
            const string Script = "return @ident";

            using (var conn = Create(allowAdmin: true))
            {
                var server = conn.GetServer(PrimaryServer, PrimaryPort);
                server.FlushAllDatabases();
                server.ScriptFlush();

                var prepared = LuaScript.Prepare(Script);
                var loaded = prepared.Load(server);

                var db = conn.GetDatabase();

                {
                    var val = loaded.Evaluate(db, new { ident = "hello" });
                    Assert.AreEqual("hello", (string)val);
                }

                {
                    var val = loaded.Evaluate(db, new { ident = 123 });
                    Assert.AreEqual(123, (int)val);
                }

                {
                    var val = loaded.Evaluate(db, new { ident = 123L });
                    Assert.AreEqual(123L, (long)val);
                }

                {
                    var val = loaded.Evaluate(db, new { ident = 1.1 });
                    Assert.AreEqual(1.1, (double)val);
                }

                {
                    var val = loaded.Evaluate(db, new { ident = true });
                    Assert.AreEqual(true, (bool)val);
                }

                {
                    var val = loaded.Evaluate(db, new { ident = new byte[] { 4, 5, 6 } });
                    Assert.IsTrue(new byte[] { 4, 5, 6 }.SequenceEqual((byte[])val));
                }
            }
        }

        [Test]
        public void LoadedLuaScriptWithKeys()
        {
            const string Script = "redis.call('set', @key, @value)";

            var log = new StringWriter();
            using (var conn = Create(allowAdmin: true))
            {
                var server = conn.GetServer(PrimaryServer, PrimaryPort);
                server.FlushAllDatabases();
                server.ScriptFlush();

                var script = LuaScript.Prepare(Script);
                var prepared = script.Load(server);

                var db = conn.GetDatabase();

                var p = new { key = (RedisKey)"testkey", value = 123 };

                prepared.Evaluate(db, p);
                var val = db.StringGet("testkey");
                Assert.AreEqual(123, (int)val);

                // no super clean way to extract this; so just abuse InternalsVisibleTo
                RedisKey[] keys;
                RedisValue[] args;
                prepared.Original.ExtractParameters(p, out keys, out args);
                Assert.IsNotNull(keys);
                Assert.AreEqual(1, keys.Length);
                Assert.AreEqual("testkey", (string)keys[0]);
            }
        }
    }
}
