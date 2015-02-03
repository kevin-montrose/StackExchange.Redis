using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StackExchange.Redis.StackExchange.Redis
{
    /// <summary>
    /// Represents a Lua script that can be executed on Redis.
    /// 
    /// Unlike normal Redis Lua scripts, LuaScript can have named parameters (prefixed by a @).
    /// Public fields and properties of the passed in object are treated as parameters.
    /// 
    /// Parameters of type RedisKey are sent to Redis as KEY (http://redis.io/commands/eval) in addition to arguments, 
    /// so as to play nicely with Redis Cluster.
    /// 
    /// All members of this class are thread safe.
    /// </summary>
    public sealed class LuaScript
    {
        // Since the mapping of "scritp text" -> LuaScript doesn't depend on any particular details of
        //    the redis connection itself, this cache is global.
        static readonly ConcurrentDictionary<string, LuaScript> Cache = new ConcurrentDictionary<string, LuaScript>();

        /// <summary>
        /// The original Lua script that was used to create this.
        /// </summary>
        public string OriginalScript { get; private set; }

        /// <summary>
        /// The Lua script that will actually be sent to Redis for execution.
        /// 
        /// All @-prefixed parameter names have been replaced at this point.
        /// </summary>
        public string ExecutableScript { get; private set; }

        // Arguments are in the order they have to passed to the script in
        internal string[] Arguments { get; private set; }

        bool HasArguments { get { return Arguments != null && Arguments.Length > 0; } }

        Hashtable ParameterMappers;

        internal LuaScript(string originalScript, string executableScript, string[] arguments)
        {
            OriginalScript = originalScript;
            ExecutableScript = executableScript;
            Arguments = arguments;

            if (HasArguments)
            {
                ParameterMappers = new Hashtable();
            }
        }

        /// <summary>
        /// Prepares a Lua script with named parameters to be run against any Redis instance.
        /// </summary>
        public static LuaScript Prepare(string script)
        {
            // It's all the same, no reason to do the work twice.
            return Cache.GetOrAdd(script, _ => ScriptParameterMapper.PrepareScript(script));
        }

        internal void ExtractParameters(object ps, out RedisKey[] keys, out RedisValue[] args)
        {
            if (HasArguments)
            {
                // TODO: better exception
                if (ps == null) throw new Exception("Script requires parameters");

                var psType = ps.GetType();
                var mapper = (Func<object, ScriptParameterMapper.ScriptParameters>)ParameterMappers[psType];
                if (ps != null && mapper == null)
                {
                    lock (ParameterMappers)
                    {
                        mapper = (Func<object, ScriptParameterMapper.ScriptParameters>)ParameterMappers[psType];
                        if (mapper == null)
                        {
                            ParameterMappers[psType] = mapper = ScriptParameterMapper.GetParameterExtractor(psType, this);
                        }
                    }
                }

                var mapped = mapper(ps);
                keys = mapped.Keys;
                args = mapped.Arguments;
            }
            else
            {
                keys = null;
                args = null;
            }
        }

        /// <summary>
        /// Evaluates this LuaScript against the given database, extracting parameters from the passed in object if any.
        /// </summary>
        public RedisResult Evaluate(IDatabase db, object ps = null, CommandFlags flags = CommandFlags.None)
        {
            RedisKey[] keys;
            RedisValue[] args;
            ExtractParameters(ps, out keys, out args);

            return db.ScriptEvaluate(ExecutableScript, keys, args, flags);
        }

        /// <summary>
        /// Evaluates this LuaScript against the given database, extracting parameters from the passed in object if any.
        /// </summary>
        public Task<RedisResult> EvaluateAsync(IDatabaseAsync db, object ps = null, CommandFlags flags = CommandFlags.None)
        {
            RedisKey[] keys;
            RedisValue[] args;
            ExtractParameters(ps, out keys, out args);

            return db.ScriptEvaluateAsync(ExecutableScript, keys, args, flags);
        }

        /// <summary>
        /// Loads this LuaScript into the given IServer so it can be run with it's SHA1 hash, instead of
        /// passing the full script on each Evaluate or EvaluateAsync call.
        /// 
        /// Note: the FireAndForget command flag cannot be set
        /// </summary>
        public LoadedLuaScript ScriptLoad(IServer server, CommandFlags flags = CommandFlags.None)
        {
            if (flags.HasFlag(CommandFlags.FireAndForget))
            {
                // TODO: Better exceptions
                throw new Exception("Loading a script cannot be FireAndForget");
            }

            var hash = server.ScriptLoad(ExecutableScript, flags);

            return new LoadedLuaScript(this, hash);
        }

        /// <summary>
        /// Loads this LuaScript into the given IServer so it can be run with it's SHA1 hash, instead of
        /// passing the full script on each Evaluate or EvaluateAsync call.
        /// 
        /// Note: the FireAndForget command flag cannot be set
        /// </summary>
        public async Task<LoadedLuaScript> ScriptLoadAsync(IServer server, CommandFlags flags = CommandFlags.None)
        {
            if (flags.HasFlag(CommandFlags.FireAndForget))
            {
                // TODO: Better exceptions
                throw new Exception("Loading a script cannot be FireAndForget");
            }

            var hash = await server.ScriptLoadAsync(ExecutableScript, flags);

            return new LoadedLuaScript(this, hash);
        }
    }

    /// <summary>
    /// Represents a Lua script that can be executed on Redis.
    /// 
    /// Unlike LuaScript, LoadedLuaScript sends the hash of it's ExecutableScript to Redis rather than pass
    /// the whole script on each call.  This requires that the script be loaded into Redis before it is used.
    /// 
    /// To create a LoadedLuaScript first create a LuaScript via LuaScript.Prepare(string), then
    /// call Load(IServer, CommandFlags) on the returned LuaScript.
    /// 
    /// Unlike normal Redis Lua scripts, LoadedLuaScript can have named parameters (prefixed by a @).
    /// Public fields and properties of the passed in object are treated as parameters.
    /// 
    /// Parameters of type RedisKey are sent to Redis as KEY (http://redis.io/commands/eval) in addition to arguments, 
    /// so as to play nicely with Redis Cluster.
    /// 
    /// All members of this class are thread safe.
    /// </summary>
    public sealed class LoadedLuaScript
    {
        /// <summary>
        /// The original script that was used to create this LoadedLuaScript.
        /// </summary>
        public string OriginalScript { get { return Original.OriginalScript; } }

        /// <summary>
        /// The script that will actually be sent to Redis for execution.
        /// </summary>
        public string ExecutableScript { get { return Original.ExecutableScript; } }

        /// <summary>
        /// The SHA1 hash of ExecutableScript.
        /// 
        /// This is sent to Redis instead of ExecutableScript during Evaluate and EvaluateAsync calls.
        /// </summary>
        public byte[] Hash { get; private set; }

        LuaScript Original;

        internal LoadedLuaScript(LuaScript original, byte[] hash)
        {
            Original = original;
            Hash = hash;
        }

        /// <summary>
        /// Evaluates this LoadedLuaScript against the given database, extracting parameters for the passed in object if any.
        /// 
        /// This method sends the SHA1 hash of the ExecutableScript instead of the script itself.  If the script has not
        /// been loaded into the passed Redis instance it will fail.
        /// </summary>
        public RedisResult Evaluate(IDatabase db, object ps = null, CommandFlags flags = CommandFlags.None)
        {
            RedisKey[] keys;
            RedisValue[] args;
            Original.ExtractParameters(ps, out keys, out args);

            return db.ScriptEvaluate(Hash, keys, args, flags);
        }

        /// <summary>
        /// Evaluates this LoadedLuaScript against the given database, extracting parameters for the passed in object if any.
        /// 
        /// This method sends the SHA1 hash of the ExecutableScript instead of the script itself.  If the script has not
        /// been loaded into the passed Redis instance it will fail.
        /// </summary>
        public Task<RedisResult> EvaluateAsync(IDatabaseAsync db, object ps = null, CommandFlags flags = CommandFlags.None)
        {
            RedisKey[] keys;
            RedisValue[] args;
            Original.ExtractParameters(ps, out keys, out args);

            return db.ScriptEvaluateAsync(Hash, keys, args, flags);
        }
    }
}
