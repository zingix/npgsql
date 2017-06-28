using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Npgsql.BackendMessages;
using Npgsql.PostgresTypes;
using Npgsql.TypeMapping;

namespace Npgsql.Json.NET
{
    class JsonHandlerFactory : TypeHandlerFactory
    {
        internal override TypeHandler Create(NpgsqlConnection conn)
            => new JsonHandler(conn);
    }

    class JsonHandler : Npgsql.TypeHandlers.TextHandler
    {
        public JsonHandler(NpgsqlConnection connection)
            : base(connection) {}

        internal override async ValueTask<T> Read<T>(ReadBuffer buf, int len, bool async, FieldDescription fieldDescription = null)
        {
            var s = await base.Read<string>(buf, len, async, fieldDescription);
            if (typeof(T) == typeof(string))
                return (T)(object)s;
            try
            {
                return JsonConvert.DeserializeObject<T>(s);
            }
            catch (Exception e)
            {
                throw new SafeReadException(e);
            }
        }

        public override int ValidateAndGetLength(object value, ref LengthCache lengthCache, NpgsqlParameter parameter = null)
        {
            var s = value as string;
            if (s == null)
            {
                s = JsonConvert.SerializeObject(value);
                if (parameter != null)
                    parameter.ConvertedValue = s;
            }
            return base.ValidateAndGetLength(s, ref lengthCache, parameter);
        }

        protected override Task Write(object value, WriteBuffer buf, LengthCache lengthCache, NpgsqlParameter parameter,
            bool async, CancellationToken cancellationToken)
        {
            if (parameter?.ConvertedValue != null)
                value = parameter.ConvertedValue;
            var s = value as string ?? JsonConvert.SerializeObject(value);
            return base.Write(s, buf, lengthCache, parameter, async, cancellationToken);
        }
    }
}
