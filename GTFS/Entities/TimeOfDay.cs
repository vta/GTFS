// The MIT License (MIT)

// Copyright (c) 2014 Ben Abelshausen

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

namespace GTFS.Entities
{
    using System;
    /// <summary>
    /// A stop time of day.
    /// </summary>
    public struct TimeOfDay : IComparable
    {
        /// <summary>
        /// Gets or sets the hours.
        /// </summary>
        public int Hours { get; set; }

        /// <summary>
        /// Gets or sets the minutes.
        /// </summary>
        public int Minutes { get; set; }

        /// <summary>
        /// Gets or sets the seconds.
        /// </summary>
        public int Seconds { get; set; }

        /// <summary>
        /// Gets the total seconds.
        /// </summary>
        public int TotalSeconds
        {
            get
            {
                return this.Hours * 60 * 60 + this.Minutes * 60 + this.Seconds;
            }
        }

        /// <summary>
        /// Creates a new time of day from total seconds.
        /// </summary>
        public static TimeOfDay FromTotalSeconds(int totalSeconds)
        {
            var hours = (int)System.Math.Floor(totalSeconds / 3600.0);
            var minutes = (int)System.Math.Floor((totalSeconds - hours * 3600.0) / 60.0);
            var seconds = (int)(totalSeconds - hours * 3600.0 - minutes * 60.0);

            return new TimeOfDay()
            {
                Hours = hours,
                Minutes = minutes,
                Seconds = seconds
            };
        }

        public static TimeOfDay FromDateTime(DateTime date)
        {
            return FromTimeSpan(date.TimeOfDay);
        }

        public static TimeOfDay FromTimeSpan(TimeSpan ts)
        {
            return FromTotalSeconds(Convert.ToInt32(ts.TotalSeconds));
        }

        /// <summary>
        /// Creates a new time of day from a string in the HH:MM:SS format.
        /// </summary>
        public static TimeOfDay FromString(string timeString)
        {
            try
            {
                TimeOfDay tod = new TimeOfDay();
                var tokens = timeString.Split(':');
                for (int i = 0; i < tokens.Length; i++)
                {
                    if (tokens[i].Contains(" "))
                    {
                        tokens[i] = tokens[i].ToCharArray()[0] + "0";
                    }
                    else if (tokens[i] == "" || tokens[i] == "  ")
                    {
                        tokens[i] = "00";
                    }
                }
                tod.Hours = int.Parse(tokens[0]);
                tod.Minutes = int.Parse(tokens[1]);
                tod.Seconds = int.Parse(tokens[2]);
                if (tod.Seconds >= 60)
                {
                    tod.Minutes += (tod.Seconds / 60);
                    tod.Seconds = tod.Seconds % 60;
                }
                if (tod.Minutes >= 60)
                {
                    tod.Hours += (tod.Minutes / 60);
                    tod.Minutes = tod.Minutes % 60;
                }

                return tod;
            }
            catch (Exception ex)
            {
                throw new ArgumentException($"Failed to parse the given string '{timeString}' to a tod: {ex.Message}");
            }
        }

        public static bool operator >(TimeOfDay a, TimeOfDay b)
        {
            return a.TotalSeconds > b.TotalSeconds;
        }

        public static bool operator <(TimeOfDay a, TimeOfDay b)
        {
            return a.TotalSeconds < b.TotalSeconds;
        }

        public static bool operator >=(TimeOfDay a, TimeOfDay b)
        {
            return a.TotalSeconds >= b.TotalSeconds;
        }

        public static bool operator <=(TimeOfDay a, TimeOfDay b)
        {
            return a.TotalSeconds <= b.TotalSeconds;
        }

        public static bool operator ==(TimeOfDay a, TimeOfDay b)
        {
            if (Object.ReferenceEquals(a, null) || Object.ReferenceEquals(b, null))
            {
                return Object.ReferenceEquals(a, null) && Object.ReferenceEquals(b, null);
            }

            return a.TotalSeconds == b.TotalSeconds;
        }

        public static bool operator !=(TimeOfDay a, TimeOfDay b)
        {
            return !(a == b);
        }

        /// <summary>
        /// Serves as a hash function.
        /// </summary>
        public override int GetHashCode()
        {
            unchecked
            {
                int hash = 61;
                hash = hash * 67 + this.Hours.GetHashCode();
                hash = hash * 67 + this.Minutes.GetHashCode();
                hash = hash * 67 + this.Seconds.GetHashCode();
                return hash;
            }
        }

        /// <summary>
        /// Returns true if the given object contains the same data.
        /// </summary>
        public override bool Equals(object obj)
        {
            if (obj is TimeOfDay)
            {
                var other = (TimeOfDay)obj;
                return this.Hours == other.Hours &&
                    this.Minutes == other.Minutes &&
                    this.Seconds == other.Seconds;
            }
            return false;
        }

        /// <summary>
        /// Compare to another
        /// </summary>
        public int CompareTo(object obj)
        {
            var other = (TimeOfDay)obj;
            return (Hours - other.Hours) * 3600 + (Minutes - other.Minutes) * 60 + (Seconds - other.Seconds);
        }

        /// <summary>
        /// To String
        /// </summary>
        public override string ToString()
        {
            string hours = Hours < 10 ? "0" + Hours : "" + Hours;
            string minutes = Minutes < 10 ? "0" + Minutes : "" + Minutes;
            string seconds = Seconds < 10 ? "0" + Seconds : "" + Seconds;
            return String.Format("{0}:{1}:{2}", hours, minutes, seconds);
        }
    }
}