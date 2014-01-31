﻿// The MIT License (MIT)

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

using GTFS.Core.Entities;
using System.Collections.Generic;

namespace GTFS.Core
{
    /// <summary>
    /// Represents an entire GTFS feed.
    /// </summary>
    public class Feed
    {
        /// <summary>
        /// Gets the list of agencies.
        /// </summary>
        public List<Agency> Agencies { get; private set; }

        /// <summary>
        /// Gets the list of stops.
        /// </summary>
        public List<Stop> Stops { get; private set; }

        /// <summary>
        /// Gets the list of routes.
        /// </summary>
        public List<Route> Routes { get; private set; }

        /// <summary>
        /// Gets the list of trips.
        /// </summary>
        public List<Trip> Trips { get; private set; }

        /// <summary>
        /// Gets the list of stop times.
        /// </summary>
        public List<StopTime> StopTimes { get; private set; }

        /// <summary>
        /// Gets the list of calendars.
        /// </summary>
        public List<Calendar> Calendars { get; private set; }

        /// <summary>
        /// Gets the list of calendar dates.
        /// </summary>
        public List<CalendarDate> CalendarDates { get; private set; }

        /// <summary>
        /// Gets the list of fare attributes.
        /// </summary>
        public List<FareAttribute> FareAttributes { get; private set; }

        /// <summary>
        /// Gets the list of fare rules.
        /// </summary>
        public List<FareRule> FareRules { get; private set; }

        /// <summary>
        /// Gets the list of shapes.
        /// </summary>
        public List<Shape> Shapes { get; private set; }

        /// <summary>
        /// Gets the list of frequencies.
        /// </summary>
        public List<Frequency> Frequencies { get; private set; }

        /// <summary>
        /// Gets the list of transfers.
        /// </summary>
        public List<Transfer> Transfers { get; private set; }

        /// <summary>
        /// Gets the feed info.
        /// </summary>
        public FeedInfo FeedInfo { get; private set; }
    }
}