using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NodaTime;

namespace Npgsql.NodaTime
{
    public struct LocalDateWithFiniteness
    {
        public LocalDateWithFiniteness(LocalDate date)
        {
            Finiteness = DateFiniteness.Finite;
            _date = date;
        }

        LocalDateWithFiniteness(DateFiniteness finiteness)
        {
            Finiteness = finiteness;
            _date = default(LocalDate);
        }

        public LocalDateWithFiniteness(int year, int month, int day)
            : this(new LocalDate(year, month, day)) { }

        public static LocalDateWithFiniteness Infinity { get; }
            = new LocalDateWithFiniteness(DateFiniteness.Infinity);

        public static LocalDateWithFiniteness NegativeInfinity { get; }
            = new LocalDateWithFiniteness(DateFiniteness.NegativeInfinity);

        public DateFiniteness Finiteness { get; }

        readonly LocalDate _date;

        public LocalDate Date
        {
            get
            {
                if (Finiteness == DateFiniteness.Finite)
                    return _date;
                throw new InvalidOperationException($"Can't retrieve date because type is {Finiteness}");
            }
        }

        public override bool Equals(object obj)
        {
            if (!(obj is LocalDateWithFiniteness))
                return false;
            var o = (LocalDateWithFiniteness)obj;
            switch (Finiteness)
            {
            case DateFiniteness.Infinity:
            case DateFiniteness.NegativeInfinity:
                return Finiteness == o.Finiteness;
            case DateFiniteness.Finite:
                return Finiteness == o.Finiteness && Date == o.Date;
            default:
                throw new ArgumentOutOfRangeException();
            }
        }

        public override int GetHashCode() => Date.GetHashCode();
    }

    public enum DateFiniteness
    {
        Finite,
        Infinity,
        NegativeInfinity
    }
}
