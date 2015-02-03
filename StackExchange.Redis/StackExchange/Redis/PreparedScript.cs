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
    /// Unlike normal Redis Lua scripts, PreparedScripts can have named parameters (prefixed by a @).
    /// Public fields and properties of the passed in object are treated as parameters.
    /// 
    /// Parameters of type RedisKey are sent to Redis as KEY (http://redis.io/commands/eval) in addition to arguments, 
    /// so as to play nicely with Redis Cluster.
    /// 
    /// All members of this class are thread safe.
    /// </summary>
    public sealed class PreparedScript
    {
        static readonly ConcurrentDictionary<string, PreparedScript> Cache = new ConcurrentDictionary<string, PreparedScript>();

        /// <summary>
        /// The original script that was used to create this PreparedScript.
        /// </summary>
        public string OriginalScript { get; private set; }

        /// <summary>
        /// The script that will actually be sent to Redis for execution.
        /// </summary>
        public string ExecutableScript { get; private set; }

        internal string[] Arguments { get; private set; }

        bool HasArguments { get { return Arguments != null && Arguments.Length > 0; } }

        Hashtable ParameterMappers;

        internal PreparedScript(string originalScript, string executableScript, string[] arguments)
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
        public static PreparedScript Prepare(string script)
        {
            return Cache.GetOrAdd(script, _ => ScriptParameterMapper.PrepareScript(script));
        }

        void ExtractParameters(object ps, out RedisKey[] keys, out RedisValue[] args)
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
        /// Evaluates this PreparedScript against the given database, extracting parameters for the passed in object if any.
        /// </summary>
        public RedisResult Evaluate(IDatabase db, object ps = null, CommandFlags flags = CommandFlags.None)
        {
            RedisKey[] keys;
            RedisValue[] args;
            ExtractParameters(ps, out keys, out args);

            return db.ScriptEvaluate(ExecutableScript, keys, args, flags);
        }

        /// <summary>
        /// Evaluates this PreparedScript against the given database, extracting parameters for the passed in object if any.
        /// </summary>
        public Task<RedisResult> EvaluateAsync(IDatabaseAsync db, object ps = null, CommandFlags flags = CommandFlags.None)
        {
            RedisKey[] keys;
            RedisValue[] args;
            ExtractParameters(ps, out keys, out args);

            return db.ScriptEvaluateAsync(ExecutableScript, keys, args, flags);
        }
    }
}
