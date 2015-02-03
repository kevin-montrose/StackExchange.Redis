using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace StackExchange.Redis.StackExchange.Redis
{
    class ScriptParameterMapper
    {
        public struct ScriptParameters
        {
            public RedisKey[] Keys;
            public RedisValue[] Arguments;

            public static readonly ConstructorInfo Cons = typeof(ScriptParameters).GetConstructor(new[] { typeof(RedisKey[]), typeof(RedisValue[]) });
            public ScriptParameters(RedisKey[] keys, RedisValue[] args)
            {
                Keys = keys;
                Arguments = args;
            }
        }

        static readonly Regex ParameterExtractor = new Regex(@"@(?<paramName>([a-z]|_)([a-z]|_|\d)*)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        static string[] ExtractParameters(string script)
        {
            var ps = ParameterExtractor.Matches(script);
            if (ps.Count == 0) return null;

            var ret = new HashSet<string>();

            for (var i = 0; i < ps.Count; i++)
            {
                var c = ps[i];
                var n = c.Groups["paramName"].Value;
                if (!ret.Contains(n)) ret.Add(n);
            }

            return ret.ToArray();
        }

        static string MakeOrdinalScriptWithoutKeys(string rawScript, string[] args)
        {
            var ps = ParameterExtractor.Matches(rawScript);
            if (ps.Count == 0) return rawScript;

            var ret = new StringBuilder();
            var upTo = 0;

            for (var i = 0; i < ps.Count; i++)
            {
                var capture = ps[i];
                var name = capture.Groups["paramName"].Value;

                var ix = capture.Index;
                ret.Append(rawScript.Substring(upTo, ix - upTo));

                var argIx = Array.IndexOf(args, name);

                if (argIx != -1)
                {
                    ret.Append("ARGV[");
                    ret.Append(argIx);
                    ret.Append("]");
                }

                upTo = capture.Index + capture.Length;
            }

            ret.Append(rawScript.Substring(upTo, rawScript.Length - upTo));

            return ret.ToString();
        }

        static void LoadMember(ILGenerator il, MemberInfo member)
        {
            // stack starts:
            // T(*?)

            var asField = member as FieldInfo;
            if (asField != null)
            {
                il.Emit(OpCodes.Ldfld, asField);        // typeof(member)
                return;
            }

            var asProp = member as PropertyInfo;
            if (asProp != null)
            {
                var getter = asProp.GetGetMethod();
                if (getter.IsVirtual)
                {
                    il.Emit(OpCodes.Callvirt, getter);  // typeof(member)
                }
                else
                {
                    il.Emit(OpCodes.Call, getter);      // typeof(member)
                }

                return;
            }

            throw new Exception("Should't be possible");
        }

        static readonly MethodInfo RedisValue_FromInt = typeof(RedisValue).GetMethod("op_Implicit", new[] { typeof(int) });
        static readonly MethodInfo RedisValue_FromNullableInt = typeof(RedisValue).GetMethod("op_Implicit", new[] { typeof(int?) });
        static readonly MethodInfo RedisValue_FromLong = typeof(RedisValue).GetMethod("op_Implicit", new[] { typeof(long) });
        static readonly MethodInfo RedisValue_FromNullableLong = typeof(RedisValue).GetMethod("op_Implicit", new[] { typeof(long?) });
        static readonly MethodInfo RedisValue_FromDouble= typeof(RedisValue).GetMethod("op_Implicit", new[] { typeof(double) });
        static readonly MethodInfo RedisValue_FromNullableDouble = typeof(RedisValue).GetMethod("op_Implicit", new[] { typeof(double?) });
        static readonly MethodInfo RedisValue_FromString = typeof(RedisValue).GetMethod("op_Implicit", new[] { typeof(string) });
        static readonly MethodInfo RedisValue_FromByteArray = typeof(RedisValue).GetMethod("op_Implicit", new[] { typeof(byte[]) });
        static readonly MethodInfo RedisValue_FromBool = typeof(RedisValue).GetMethod("op_Implicit", new[] { typeof(bool) });
        static readonly MethodInfo RedisValue_FromNullableBool = typeof(RedisValue).GetMethod("op_Implicit", new[] { typeof(bool?) });
        static readonly MethodInfo RedisKey_AsRedisValue = typeof(RedisKey).GetMethod("AsRedisValue", BindingFlags.NonPublic | BindingFlags.Instance);
        static void ConvertToRedisValue(MemberInfo member, ILGenerator il, ref LocalBuilder redisKeyLoc)
        {
            // stack starts:
            // typeof(member)

            var t = member is FieldInfo ? ((FieldInfo)member).FieldType : ((PropertyInfo)member).PropertyType;

            if (t == typeof(RedisValue))
            {
                // They've already converted for us, don't do anything
                return;
            }

            if (t == typeof(RedisKey))
            {
                redisKeyLoc = redisKeyLoc ?? il.DeclareLocal(typeof(RedisKey));
                il.Emit(OpCodes.Stloc, redisKeyLoc);            // --empty--
                il.Emit(OpCodes.Ldloca, redisKeyLoc);           // RedisKey*
                il.Emit(OpCodes.Call, RedisKey_AsRedisValue);   // RedisValue
                return;
            }

            MethodInfo convertOp = null;
            if (t == typeof(int)) convertOp = RedisValue_FromInt;
            if (t == typeof(int?)) convertOp = RedisValue_FromNullableInt;
            if (t == typeof(long)) convertOp = RedisValue_FromLong;
            if (t == typeof(long?)) convertOp = RedisValue_FromNullableLong;
            if (t == typeof(double)) convertOp = RedisValue_FromDouble;
            if (t == typeof(double?)) convertOp = RedisValue_FromNullableDouble;
            if (t == typeof(string)) convertOp = RedisValue_FromString;
            if (t == typeof(byte[])) convertOp = RedisValue_FromByteArray;
            if (t == typeof(bool)) convertOp = RedisValue_FromBool;
            if (t == typeof(bool?)) convertOp = RedisValue_FromNullableBool;

            if (convertOp == null)
            {
                // TODO: better exception type!
                throw new Exception("Cannot convert member [" + member.Name + "] on [" + member.DeclaringType.FullName + "] to a RedisValue");
            }

            il.Emit(OpCodes.Call, convertOp);

            // stack ends:
            // RedisValue
        }

        /// <summary>
        /// Turns a script with @namedParameters into a PreparedScript that can be executed
        /// against a given IDatabase(Async) object
        /// </summary>
        public static PreparedScript PrepareScript(string script)
        {
            var ps = ExtractParameters(script);
            var ordinalScript = MakeOrdinalScriptWithoutKeys(script, ps);

            return new PreparedScript(script, ordinalScript, ps);
        }

        /// <summary>
        /// Creates a Func that extracts parameters from the given type for use by a PreparedScript.
        /// 
        /// Members that are RedisKey's get extracted to be passed in as keys to redis; all members that
        /// appear in the script get extracted as RedisValue arguments to be sent up as args.
        /// 
        /// We send all values as arguments so we don't have to prepare the same script for different parameter
        /// types.
        /// </summary>
        public static Func<object, ScriptParameters> GetParameterExtractor(Type t, PreparedScript script)
        {
            var keys = new List<MemberInfo>();
            var args = new List<MemberInfo>();

            for (var i = 0; i < script.Arguments.Length; i++)
            {
                var argName = script.Arguments[i];
                var member = t.GetMember(argName).Where(m => m is PropertyInfo || m is FieldInfo).SingleOrDefault();
                
                // TODO: better exception
                if (member == null)
                    throw new Exception("Expected property or field on " + t.FullName + " with name " + argName);

                var memberType = member is FieldInfo ? ((FieldInfo)member).FieldType : ((PropertyInfo)member).PropertyType;

                if (memberType == typeof(RedisKey))
                {
                    keys.Add(member);
                }

                args.Add(member);
            }

            var dyn = new DynamicMethod("ParameterExtractor_" + t.FullName + "_" + script.OriginalScript.GetHashCode(), typeof(ScriptParameters), new[] { typeof(object) }, restrictedSkipVisibility: true);
            var il = dyn.GetILGenerator();

            // only init'd if we use it
            LocalBuilder redisKeyLoc = null;
            var loc = il.DeclareLocal(t);
            il.Emit(OpCodes.Ldarg_0);               // object
            if (t.IsValueType)
            {
                il.Emit(OpCodes.Unbox_Any, t);      // T
            }
            else
            {
                il.Emit(OpCodes.Castclass, t);      // T
            }
            il.Emit(OpCodes.Stloc, loc);            // --empty--

            if (keys.Count == 0)
            {
                // if there are no keys, don't allocate
                il.Emit(OpCodes.Ldnull);                    // null
            }
            else
            {
                il.Emit(OpCodes.Ldc_I4, keys.Count);        // int
                il.Emit(OpCodes.Newarr, typeof(RedisKey));  // RedisKey[]
            }

            for (var i = 0; i < keys.Count; i++)
            {
                il.Emit(OpCodes.Dup);                       // RedisKey[] RedisKey[]
                il.Emit(OpCodes.Ldc_I4, i);                 // RedisKey[] RedisKey[] int
                if (t.IsValueType)
                {
                    il.Emit(OpCodes.Ldloca, loc);           // RedisKey[] RedisKey[] int T*
                }
                else
                {
                    il.Emit(OpCodes.Ldloc, loc);            // RedisKey[] RedisKey[] int T
                }
                LoadMember(il, keys[i]);                    // RedisKey[] RedisKey[] int RedisKey
                il.Emit(OpCodes.Stelem, typeof(RedisKey));  // RedisKey[]
            }

            if (args.Count == 0)
            {
                // if there are no args, don't allocate
                il.Emit(OpCodes.Ldnull);                        // RedisKey[] null
            }
            else
            {
                il.Emit(OpCodes.Ldc_I4, args.Count);            // RedisKey[] int
                il.Emit(OpCodes.Newarr, typeof(RedisValue));    // RedisKey[] RedisValue[]
            }

            for (var i = 0; i < args.Count; i++)
            {
                il.Emit(OpCodes.Dup);                       // RedisKey[] RedisValue[] RedisValue[]
                il.Emit(OpCodes.Ldc_I4, i);                 // RedisKey[] RedisValue[] RedisValue[] int
                if (t.IsValueType)
                {
                    il.Emit(OpCodes.Ldloca, loc);           // RedisKey[] RedisValue[] RedisValue[] int T*
                }
                else
                {
                    il.Emit(OpCodes.Ldloc, loc);            // RedisKey[] RedisValue[] RedisValue[] int T
                }
                
                var member = args[i];
                LoadMember(il, member);                             // RedisKey[] RedisValue[] RedisValue[] int memberType
                ConvertToRedisValue(member, il, ref redisKeyLoc);   // RedisKey[] RedisValue[] RedisValue[] int RedisValue

                il.Emit(OpCodes.Stelem, typeof(RedisValue));        // RedisKey[] RedisValue[]
            }

            il.Emit(OpCodes.Newobj, ScriptParameters.Cons); // ScriptParameters
            il.Emit(OpCodes.Ret);                           // --empty--

            var ret = (Func<object, ScriptParameters>)dyn.CreateDelegate(typeof(Func<object, ScriptParameters>));

            return ret;
        }
    }
}
