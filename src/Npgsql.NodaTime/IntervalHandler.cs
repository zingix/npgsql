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
    public class IntervalHandlerFactory : TypeHandlerFactory
    {
        // Check for the legacy floating point timestamps feature
        internal override TypeHandler Create(NpgsqlConnection conn)
            => new IntervalHandler(
                conn.Connector.BackendParams.TryGetValue("integer_datetimes", out var s)
                && s == "on");
    }

    class IntervalHandler : SimpleTypeHandler<Period>
    {
        /// <summary>
        /// A deprecated compile-time option of PostgreSQL switches to a floating-point representation of some date/time
        /// fields. Npgsql (currently) does not support this mode.
        /// </summary>
        readonly bool _integerFormat;

        public IntervalHandler(bool integerFormat)
        {
            _integerFormat = integerFormat;
        }

        public override Period Read(ReadBuffer buf, int len, FieldDescription fieldDescription = null)
        {
            CheckIntegerFormat();
            var microsecondsInDay = buf.ReadInt64();
            var days = buf.ReadInt32();
            var totalMonths = buf.ReadInt32();

            // Nodatime will normalize most things (i.e. nanoseconds to milliseconds, seconds...)
            // but it will not normalize months to years.
            var months = totalMonths % 12;
            var years = totalMonths / 12;

            return new PeriodBuilder
            {
                Nanoseconds = microsecondsInDay * 1000,
                Days = days,
                Months = months,
                Years = years
            }.Build().Normalize();
        }

        public override int ValidateAndGetLength(object value, NpgsqlParameter parameter = null)
        {
            CheckIntegerFormat();
            if (!(value is Period))
                throw CreateConversionException(value.GetType());
            return 16;
        }

        protected override void Write(object value, WriteBuffer buf, NpgsqlParameter parameter = null)
        {
            var period = (Period)value;

            var microsecondsInDay =
                (((period.Hours * NodaConstants.MinutesPerHour + period.Minutes) * NodaConstants.SecondsPerMinute + period.Seconds) * NodaConstants.MillisecondsPerSecond + period.Milliseconds) * 1000 +
                period.Nanoseconds / 1000;  // Take the microseconds, discard the nanosecond remainder
            buf.WriteInt64(microsecondsInDay);
            buf.WriteInt32(period.Weeks * 7 + period.Days);     // days
            buf.WriteInt32(period.Years * 12 + period.Months);  // months
        }

        void CheckIntegerFormat()
        {
            if (!_integerFormat)
                throw new NotSupportedException("Old floating point representation for timestamps not supported");
        }
    }
}
