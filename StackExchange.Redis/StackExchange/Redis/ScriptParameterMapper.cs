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
        public class ExecutableScript
        {
            public string OriginalScript { get; private set; }
            public string OriginalSHA1 { get; private set; }

            public string PreparedScript { get; private set; }
            public string PreparedSHA1 { get; set; }

            public RedisKey[] Keys { get; private set; }
            public RedisValue[] Arguments { get; private set; }

            public static readonly ConstructorInfo Cons = typeof(ExecutableScript).GetConstructor(new[] { typeof(string), typeof(string), typeof(string), typeof(string), typeof(RedisKey[]), typeof(RedisValue[]) });
            public ExecutableScript(string origScript, string origSha1, string prepScript, string prepSha1, RedisKey[] keys, RedisValue[] args)
            {
                OriginalScript = origScript;
                OriginalSHA1 = origSha1;
                PreparedScript = prepScript;
                PreparedSHA1 = prepSha1;
                Keys = keys;
                Arguments = args;
            }
        }

        static readonly Regex ParameterExtractor = new Regex(@"@(?<paramName>([a-z]|_)([a-z]|_|\d)*)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        static void ExtractKeysAndArguments(Type t, string script, out Dictionary<string, MemberInfo> keys, out Dictionary<string, MemberInfo> args)
        {
            var ps = ParameterExtractor.Matches(script);

            keys = new Dictionary<string, MemberInfo>();
            args = new Dictionary<string, MemberInfo>();

            for (var i = 0; i < ps.Count; i++)
            {
                var capture = ps[i];
                var name = capture.Groups["paramName"].Value;

                // already seen it
                if (keys.ContainsKey(name) || args.ContainsKey(name)) continue;

                var matchingMember = t.GetMember(name).Where(m => m is FieldInfo || m is PropertyInfo).SingleOrDefault();
                if (matchingMember == null)
                {
                    // TODO: better exception type!
                    throw new Exception("No member [" + name + "] found on " + t.FullName + " when extracting parameters for script");
                }

                var asProperty = matchingMember as PropertyInfo;
                if (asProperty != null && !asProperty.CanRead)
                {
                    // TODO: better exception type!
                    throw new Exception("Member [" + name + "] cannot be read on " + t.FullName + ", cannot be used as a parameter for script");
                }

                var asField = matchingMember as FieldInfo;

                var memberType = asField != null ? asField.FieldType : asProperty.PropertyType;

                if (memberType == typeof(RedisKey))
                {
                    keys[name] = matchingMember;
                }
                else
                {
                    args[name] = matchingMember;
                }
            }
        }

        static void MakeOrdinalScript(string rawScript, Dictionary<string, MemberInfo> keys, Dictionary<string, MemberInfo> args, out string rawScriptSha1, out string finalScript, out string finalSha1, out List<MemberInfo> keyMembersInOrder, out List<MemberInfo> argMembersInOrder)
        {
            var ps = ParameterExtractor.Matches(rawScript);

            var keysInOrder = keys.Keys.OrderBy(k => k).ToList();
            keyMembersInOrder = keys.OrderBy(kv => kv.Key).Select(kv => kv.Value).ToList();
            var argsInOrder = args.Keys.OrderBy(k => k).ToList();
            argMembersInOrder = args.OrderBy(kv => kv.Key).Select(kv => kv.Value).ToList();

            var ret = new StringBuilder();
            var upTo = 0;

            for(var i = 0; i < ps.Count; i++)
            {
                var capture = ps[i];
                var name = capture.Groups["paramName"].Value;

                var ix = capture.Index;
                ret.Append(rawScript.Substring(upTo, ix - upTo));

                if(keys.ContainsKey(name))
                {
                    ret.Append("KEYS[");
                    var keyIx = keysInOrder.IndexOf(name)+1;
                    ret.Append(keyIx);
                    ret.Append("]");
                }
                else
                {
                    ret.Append("ARGV[");
                    var argIx = argsInOrder.IndexOf(name)+1;
                    ret.Append(argIx);
                    ret.Append("]");
                }

                upTo = capture.Index+capture.Length;
            }

            ret.Append(rawScript.Substring(upTo, rawScript.Length - upTo));

            finalScript = ret.ToString();

            using (var hasher = SHA1.Create())
            {
                var rawBytes = Encoding.UTF8.GetBytes(rawScript);
                var rawHash = hasher.ComputeHash(rawBytes);

                var finalBytes = Encoding.UTF8.GetBytes(finalScript);
                var finalHash = hasher.ComputeHash(finalBytes);

                // TODO: A little less garbage here would be nice
                rawScriptSha1 = string.Join("", rawHash.Select(b => b.ToString("x2")));
                finalSha1 = string.Join("", finalHash.Select(b => b.ToString("x2")));
            }
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

        static void LoadKeys(LocalBuilder loc, ILGenerator il, List<MemberInfo> keysInOrder)
        {
            // stack starts:
            // --empty--

            var numKeys = keysInOrder.Count;
            var tRedisKey = typeof(RedisKey);

            il.Emit(OpCodes.Ldc_I4, numKeys);           // int
            il.Emit(OpCodes.Newarr, tRedisKey);         // RedisKey[]

            for (var i = 0; i < numKeys; i++)
            {
                var member = keysInOrder[i];

                il.Emit(OpCodes.Dup);                   // RedisKey[] RedisKey[]
                il.Emit(OpCodes.Ldc_I4, i);             // RedisKey[] RedisKey[] int
                if (loc.LocalType.IsValueType)
                {
                    il.Emit(OpCodes.Ldloca, loc);       // RedisKey[] RedisKey[] int T*
                }
                else
                {
                    il.Emit(OpCodes.Ldloc, loc);        // RedisKey[] RedisKey[] int T
                }

                // because keys *must* be of type RedisKey, we don't have to do any conversions
                LoadMember(il, member);                 // RedisKey[] RedisKey[] int RedisKey

                il.Emit(OpCodes.Stelem, tRedisKey);     // RedisKey[]
            }

            // stack end:
            // RedisKey[]
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
        static void ConvertToRedisValue(MemberInfo member, ILGenerator il)
        {
            // stack starts:
            // typeof(member)

            var t = member is FieldInfo ? ((FieldInfo)member).FieldType : ((PropertyInfo)member).PropertyType;

            if (t == typeof(RedisValue))
            {
                // They've already converted for us, don't do anything
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

        static void LoadArguments(LocalBuilder loc, ILGenerator il, List<MemberInfo> argsInOrder)
        {
            // stack starts:
            // --empty--

            var numArgs = argsInOrder.Count;
            var tRedisVal = typeof(RedisValue);

            il.Emit(OpCodes.Ldc_I4, numArgs);           // RedisKey[] int
            il.Emit(OpCodes.Newarr, tRedisVal);         // RedisKey[] RedisValue[]

            for (var i = 0; i < numArgs; i++)
            {
                var member = argsInOrder[i];

                il.Emit(OpCodes.Dup);                   // RedisKey[] RedisValue[] RedisValue[]
                il.Emit(OpCodes.Ldc_I4, i);             // RedisKey[] RedisValue[] RedisValue[] int

                if (loc.LocalType.IsValueType)
                {
                    il.Emit(OpCodes.Ldloca, loc);       // RedisKey[] RedisValue[] RedisValue[] int T*
                }
                else
                {
                    il.Emit(OpCodes.Ldloc, loc);        // RedisKey[] RedisValue[] RedisValue[] int T
                }

                LoadMember(il, member);                 // RedisKey[] RedisValue[] RedisValue[] int typeof(member)
                ConvertToRedisValue(member, il);        // RedisKey[] RedisValue[] RedisValue[] int RedisValue

                il.Emit(OpCodes.Stelem, tRedisVal);     // RedisKey[] RedisValue[]
            }

            // stack end:
            // RedisValue[]
        }

        /// <summary>
        /// Returns a Func that can be cached.  This func projects all parameters in script (marked by @myParamName) 
        /// into a PreparedScript object.
        /// 
        /// RedisKey members on T are considered keys for the purposes of Eval calls, all other members are considered
        /// arguments.
        /// </summary>
        public static Func<T, ExecutableScript> MapScript<T>(string script)
        {
            var t = typeof(T);
            Dictionary<string, MemberInfo> keys, args;
            ExtractKeysAndArguments(t, script, out keys, out args);

            string scriptSha1, finalScript, finalSha1;
            List<MemberInfo> keysInOrder, argsInOrder;
            MakeOrdinalScript(script, keys, args, out scriptSha1, out finalScript, out finalSha1, out keysInOrder, out argsInOrder);

            var dyn = new DynamicMethod("MapScript_" + t.FullName + "_" + script.GetHashCode(), typeof(ExecutableScript), new[] { t }, restrictedSkipVisibility: true);
            var il = dyn.GetILGenerator();

            var loc = il.DeclareLocal(t);
            il.Emit(OpCodes.Ldarg_0);                       // typeof(T)
            il.Emit(OpCodes.Stloc, loc);                    // --empty--

            il.Emit(OpCodes.Ldstr, script);                 // string
            il.Emit(OpCodes.Ldstr, scriptSha1);             // string string
            il.Emit(OpCodes.Ldstr, finalScript);            // string string string
            il.Emit(OpCodes.Ldstr, finalSha1);              // string string string string

            LoadKeys(loc, il, keysInOrder);                 // string string string string RedisKey[]
            LoadArguments(loc, il, argsInOrder);            // string string string string RedisKey[] RedisValue[]

            il.Emit(OpCodes.Newobj, ExecutableScript.Cons); // ExecutableScript
            il.Emit(OpCodes.Ret);                           // --empty--

            var ret = (Func<T, ExecutableScript>)dyn.CreateDelegate(typeof(Func<T, ExecutableScript>));

            return ret;
        }
    }
}
