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
