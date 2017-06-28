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
    public class TimeTzHandlerFactory : TypeHandlerFactory
    {
        // Check for the legacy floating point timestamps feature
        internal override TypeHandler Create(NpgsqlConnection conn)
            => new TimeTzHandler(
                conn.Connector.BackendParams.TryGetValue("integer_datetimes", out var s)
                && s == "on");
    }

    class TimeTzHandler : SimpleTypeHandler<OffsetDateTime>
    {
        /// <summary>
        /// A deprecated compile-time option of PostgreSQL switches to a floating-point representation of some date/time
        /// fields. Npgsql (currently) does not support this mode.
        /// </summary>
        readonly bool _integerFormat;

        public TimeTzHandler(bool integerFormat)
        {
            _integerFormat = integerFormat;
        }

        public override OffsetDateTime Read(ReadBuffer buf, int len, FieldDescription fieldDescription = null)
        {
            CheckIntegerFormat();

            // Adjust from 1 microsecond to 100ns. Time zone (in seconds) is inverted.
            var dateTime = new global::NodaTime.LocalDate() + LocalTime.FromTicksSinceMidnight(buf.ReadInt64() * 10);
            var offset = Offset.FromSeconds(-buf.ReadInt32());
            return new OffsetDateTime(dateTime, offset);
        }

        public override int ValidateAndGetLength(object value, NpgsqlParameter parameter = null)
        {
            CheckIntegerFormat();
            if (!(value is OffsetDateTime))
                throw CreateConversionException(value.GetType());
            var v = (OffsetDateTime)value;
            if (v.Date != default(global::NodaTime.LocalDate))
                throw new InvalidCastException("Date component must be empty for timetz");
            return 12;
        }

        protected override void Write(object value, WriteBuffer buf, NpgsqlParameter parameter = null)
        {
            var v = (OffsetDateTime)value;
            buf.WriteInt64(v.TickOfDay / 10);
            buf.WriteInt32(-(int)(v.Offset.Ticks / NodaConstants.TicksPerSecond));
        }

        void CheckIntegerFormat()
        {
            if (!_integerFormat)
                throw new NotSupportedException("Old floating point representation for timestamps not supported");
        }
    }
}
