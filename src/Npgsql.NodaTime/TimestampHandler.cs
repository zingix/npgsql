using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NodaTime;
using Npgsql.BackendMessages;
using Npgsql.PostgresTypes;
using Npgsql.TypeMapping;

namespace Npgsql.NodaTime
{
    public class TimestampHandlerFactory : TypeHandlerFactory
    {
        // Check for the legacy floating point timestamps feature
        internal override TypeHandler Create(NpgsqlConnection conn)
            => new TimestampHandler(
                conn.Connector.BackendParams.TryGetValue("integer_datetimes", out var s)
                && s == "on");
    }

    class TimestampHandler : SimpleTypeHandler<LocalDateTime>
    {
        /// <summary>
        /// A deprecated compile-time option of PostgreSQL switches to a floating-point representation of some date/time
        /// fields. Npgsql (currently) does not support this mode.
        /// </summary>
        readonly bool _integerFormat;

        public TimestampHandler(bool integerFormat)
        {
            _integerFormat = integerFormat;
        }

        public override LocalDateTime Read(ReadBuffer buf, int len, FieldDescription fieldDescription = null)
        {
            CheckIntegerFormat();
            throw new NotImplementedException();
        }

        public override int ValidateAndGetLength(object value, NpgsqlParameter parameter = null)
        {
            CheckIntegerFormat();
            throw new NotImplementedException();
        }

        protected override void Write(object value, WriteBuffer buf, NpgsqlParameter parameter = null)
        {
            throw new NotImplementedException();
        }

        void CheckIntegerFormat()
        {
            if (!_integerFormat)
                throw new NotSupportedException("Old floating point representation for timestamps not supported");
        }
    }
}
