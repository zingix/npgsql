using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NodaTime;
using Npgsql.BackendMessages;
using Npgsql.PostgresTypes;
using Npgsql.TypeHandlers.DateTimeHandlers;
using Npgsql.TypeMapping;

namespace Npgsql.NodaTime
{
    class DateHandlerFactory : TypeHandlerFactory
    {
        internal override TypeHandler Create(NpgsqlConnection conn)
            => new DateHandler(conn.Connector.ConvertInfinityDateTime);
    }

    sealed class DateHandler : SimpleTypeHandlerWithPsv<LocalDate, LocalDateWithFiniteness>
    {
        /// <summary>
        /// Whether to convert positive and negative infinity values to LocalTime.{Max,Min}Value when
        /// a LocalTime is requested
        /// </summary>
        readonly bool _convertInfinityDateTime;

        public DateHandler(bool convertInfinityDateTime)
        {
            _convertInfinityDateTime = convertInfinityDateTime;
        }

        public override LocalDate Read(ReadBuffer buf, int len, FieldDescription fieldDescription = null)
        {
            // TODO: Convert directly to DateTime without passing through NpgsqlDate?
            var withFiniteness = ReadPsv(buf, len, fieldDescription);
            switch (withFiniteness.Finiteness)
            {
            case DateFiniteness.Finite:
                return withFiniteness.Date;
            case DateFiniteness.Infinity:
                if (!_convertInfinityDateTime)
                    throw new SafeReadException(new InvalidCastException("Can't convert infinite date values to LocalTime"));
                // TODO: no LocalDate.MaxValue?
                throw new NotImplementedException();
            case DateFiniteness.NegativeInfinity:
                if (!_convertInfinityDateTime)
                    throw new SafeReadException(new InvalidCastException("Can't convert infinite date values to LocalTime"));
                // TODO: no LocalDate.MaxValue?
                throw new NotImplementedException();
            default:
                throw new ArgumentOutOfRangeException();
            }
        }

        internal override LocalDateWithFiniteness ReadPsv(ReadBuffer buf, int len, FieldDescription fieldDescription = null)
        {
            var binDate = buf.ReadInt32();
            switch (binDate)
            {
            case int.MaxValue:
                return LocalDateWithFiniteness.Infinity;
            case int.MinValue:
                return LocalDateWithFiniteness.NegativeInfinity;
            default:
                return new LocalDateWithFiniteness(new LocalDate().PlusDays(binDate + 730119));
            }
        }

        public override int ValidateAndGetLength(object value, NpgsqlParameter parameter = null)
        {
            if (!(value is LocalDate) && !(value is LocalDateWithFiniteness))
                throw CreateConversionException(value.GetType());
            return 4;

        }

        protected override void Write(object value, WriteBuffer buf, NpgsqlParameter parameter = null)
        {
            LocalDate date;
            if (value is LocalDateWithFiniteness)
            {
                var withFiniteness = (LocalDateWithFiniteness)value;
                switch (withFiniteness.Finiteness)
                {
                case DateFiniteness.Finite:
                    date = withFiniteness.Date;
                    break;
                case DateFiniteness.Infinity:
                    buf.WriteInt32(int.MaxValue);
                    return;
                case DateFiniteness.NegativeInfinity:
                    buf.WriteInt32(int.MinValue);
                    return;
                default:
                    throw new ArgumentOutOfRangeException();
                }
            }
            else
            {
                date = (LocalDate)value;
            }

            var totalDaysSinceEra = Period.Between(default(LocalDate), date, PeriodUnits.Days).Days;
            buf.WriteInt32(totalDaysSinceEra - 730119);
        }
    }
}
