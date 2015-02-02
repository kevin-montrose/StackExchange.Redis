using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StackExchange.Redis.StackExchange.Redis
{
    /// <summary>
    /// Represents a Lua script that can be executed on Redis.
    /// 
    /// Unlike Redis itself, PreparedScripts can have named parameters (prefixed by a @).
    /// Parameters are passed in as objects, where public fields and properties are treated as parameters.
    /// 
    /// Parameters of type RedisKey are sent to Redis as KEY (http://redis.io/commands/eval) in addition to arguments, 
    /// so as to play nicely with Redis Cluster.
    /// </summary>
    public sealed class PreparedScript
    {
        /// <summary>
        /// The original script that was used to create this PreparedScript.
        /// </summary>
        public string OriginalScript { get; private set; }

        /// <summary>
        /// The script that will actually be sent to Redis for execution.
        /// </summary>
        public string ExecutableScript { get; private set; }

        internal string[] Arguments { get; private set; }
        Hashtable ParameterMappers;

        internal PreparedScript(string originalScript, string executableScript, string[] arguments)
        {
            OriginalScript = originalScript;
            ExecutableScript = executableScript;
            Arguments = arguments;

            ParameterMappers = new Hashtable();
        }

        void ExtractParameters(object ps, out RedisKey[] keys, out RedisValue[] args)
        {
            if (ps == null && Arguments.Length != 0)
            {
                // TODO: better exception
                throw new Exception("Script requires parameters");
            }

            var psType = ps != null ? ps.GetType() : null;
            var mapper = ps != null ? (Func<object, ScriptParameterMapper.ScriptParameters>)ParameterMappers[psType] : null;
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

            keys = null;
            args = null;
            if (mapper != null)
            {
                var mapped = mapper(ps);
                keys = mapped.Keys;
                args = mapped.Arguments;
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
