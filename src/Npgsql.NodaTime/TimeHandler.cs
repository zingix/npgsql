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
    public class TimeHandlerFactory : TypeHandlerFactory
    {
        // Check for the legacy floating point timestamps feature
        internal override TypeHandler Create(NpgsqlConnection conn)
            => new TimeHandler(
                conn.Connector.BackendParams.TryGetValue("integer_datetimes", out var s)
                && s == "on");
    }

    class TimeHandler : SimpleTypeHandler<LocalTime>
    {
        /// <summary>
        /// A deprecated compile-time option of PostgreSQL switches to a floating-point representation of some date/time
        /// fields. Npgsql (currently) does not support this mode.
        /// </summary>
        readonly bool _integerFormat;

        public TimeHandler(bool integerFormat)
        {
            _integerFormat = integerFormat;
        }

        public override LocalTime Read(ReadBuffer buf, int len, FieldDescription fieldDescription = null)
        {
            CheckIntegerFormat();

            // PostgreSQL time resolution == 1 microsecond == 10 ticks
            return LocalTime.FromTicksSinceMidnight(buf.ReadInt64() * 10);
        }

        public override int ValidateAndGetLength(object value, NpgsqlParameter parameter = null)
        {
            CheckIntegerFormat();
            if (!(value is LocalTime))
                throw CreateConversionException(value.GetType());
            return 8;
        }

        protected override void Write(object value, WriteBuffer buf, NpgsqlParameter parameter = null)
            => buf.WriteInt64(((LocalTime)value).TickOfDay / 10);

        void CheckIntegerFormat()
        {
            if (!_integerFormat)
                throw new NotSupportedException("Old floating point representation for timestamps not supported");
        }
    }
}
