#region License
// The PostgreSQL License
//
// Copyright (C) 2017 The Npgsql Development Team
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written
// agreement is hereby granted, provided that the above copyright notice
// and this paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE NPGSQL DEVELOPMENT TEAM BE LIABLE TO ANY PARTY
// FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES,
// INCLUDING LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
// DOCUMENTATION, EVEN IF THE NPGSQL DEVELOPMENT TEAM HAS BEEN ADVISED OF
// THE POSSIBILITY OF SUCH DAMAGE.
//
// THE NPGSQL DEVELOPMENT TEAM SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
// AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS
// ON AN "AS IS" BASIS, AND THE NPGSQL DEVELOPMENT TEAM HAS NO OBLIGATIONS
// TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
#endregion

using System;
using NodaTime;
using Npgsql.BackendMessages;
using Npgsql.TypeMapping;

namespace Npgsql.NodaTime
{
    class DateHandlerFactory : TypeHandlerFactory
    {
        protected override TypeHandler Create(NpgsqlConnection conn)
          => new DateHandler(false);
        //=> new DateHandler(conn.Connector.ConvertInfinityDateTime);
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

        public override LocalDate Read(NpgsqlReadBuffer buf, int len, FieldDescription fieldDescription = null)
        {
            // TODO: Convert directly to DateTime without passing through NpgsqlDate?
            var withFiniteness = ReadPsv(buf, len, fieldDescription);
            switch (withFiniteness.Finiteness)
            {
            case DateFiniteness.Finite:
                return withFiniteness.Date;
            case DateFiniteness.Infinity:
                if (!_convertInfinityDateTime)
                    throw new NpgsqlSafeReadException(new InvalidCastException("Can't convert infinite date values to LocalTime"));
                // TODO: no LocalDate.MaxValue?
                throw new NotImplementedException();
            case DateFiniteness.NegativeInfinity:
                if (!_convertInfinityDateTime)
                    throw new NpgsqlSafeReadException(new InvalidCastException("Can't convert infinite date values to LocalTime"));
                // TODO: no LocalDate.MaxValue?
                throw new NotImplementedException();
            default:
                throw new ArgumentOutOfRangeException();
            }
        }

        protected override LocalDateWithFiniteness ReadPsv(NpgsqlReadBuffer buf, int len, FieldDescription fieldDescription = null)
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

        protected override int ValidateAndGetLength(object value, NpgsqlParameter parameter = null)
        {
            if (!(value is LocalDate) && !(value is LocalDateWithFiniteness))
                throw CreateConversionException(value.GetType());
            return 4;

        }

        protected override void Write(object value, NpgsqlWriteBuffer buf, NpgsqlParameter parameter = null)
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
