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

using GTFS.Attributes;
using GTFS.Entities.Enumerations;
using System;

namespace GTFS.Entities
{
    /// <summary>
    /// Represents a stop time. Times that a vehicle arrives at and departs from individual stops for each trip.
    /// </summary>
    [FileName("stop_times")]
    public class StopTime : GTFSEntity, IComparable
    {
        /// <summary>
        /// Gets or sets a trip.
        /// </summary>
        [Required]
        [FieldName("trip_id")]
        public string TripId { get; set; }

        /// <summary>
        /// Gets or sets the arrival time at a specific stop for a specific trip on a route. The time is measured from "noon minus 12h" (effectively midnight, except for days on which daylight savings time changes occur) at the beginning of the service date. For times occurring after midnight on the service date, enter the time as a value greater than 24:00:00 in HH:MM:SS local time for the day on which the trip schedule begins. If you don't have separate times for arrival and departure at a stop, enter the same value for arrival_time and departure_time.
        /// </summary>
        [Required]
        [FieldName("arrival_time")]
        public TimeOfDay? ArrivalTime { get; set; }

        /// <summary>
        /// Gets or sets the departure time from a specific stop for a specific trip on a route. The time is measured from "noon minus 12h" (effectively midnight, except for days on which daylight savings time changes occur) at the beginning of the service date. For times occurring after midnight on the service date, enter the time as a value greater than 24:00:00 in HH:MM:SS local time for the day on which the trip schedule begins. If you don't have separate times for arrival and departure at a stop, enter the same value for arrival_time and departure_time.
        /// </summary>
        [Required]
        [FieldName("departure_time")]
        public TimeOfDay? DepartureTime { get; set; }

        /// <summary>
        /// Gets or sets a stop. Multiple routes may use the same stop. If location_type is used in stops.txt, all stops referenced in stop_times.txt must have location_type of 0.
        /// </summary>
        [Required]
        [FieldName("stop_id")]
        public string StopId { get; set; }

        /// <summary>
        /// Gets or sets the order of the stop for a particular trip. The values for stop_sequence must be non-negative integers, and they must increase along the trip.
        /// </summary>
        [Required]
        [FieldName("stop_sequence")]
        public uint StopSequence { get; set; }

        /// <summary>
        /// Gets or sets the text that appears on a sign that identifies the trip's destination to passengers. Use this field to override the default trip_headsign when the headsign changes between stops. If this headsign is associated with an entire trip, use trip_headsign instead.
        /// </summary>
        [FieldName("stop_headsign")]
        public string StopHeadsign { get; set; }

        /// <summary>
        /// Gets or sets the pickup type field that indicates whether passengers are picked up at a stop as part of the normal schedule or whether a pickup at the stop is not available. This field also allows the transit agency to indicate that passengers must call the agency or notify the driver to arrange a pickup at a particular stop.
        /// </summary>
        [FieldName("pickup_type")]
        public PickupType? PickupType { get; set; }

        /// <summary>
        /// Gets or sets the drop off type field that indicates whether passengers are dropped off at a stop as part of the normal schedule or whether a drop off at the stop is not available. This field also allows the transit agency to indicate that passengers must call the agency or notify the driver to arrange a drop off at a particular stop. 
        /// </summary>
        [FieldName("drop_off_type")]
        public DropOffType? DropOffType { get; set; }

        /// <summary>
        /// Gets or sets a distance from the first shape point.
        /// </summary>
        [FieldName("shape_dist_traveled")]
        public string ShapeDistTravelled { get; set; }

        /// <summary>
        /// Gets or sets number of passengers that boarded at this stop during this trip.
        /// </summary>
        [FieldName("passenger_boarding")]
        public int? PassengerBoarding { get; set; }

        /// <summary>
        /// Gets or sets number of passengers that alighted at this stop during this trip.
        /// </summary>
        [FieldName("passenger_alighting")]
        public int? PassengerAlighting { get; set; }

        /// <summary>
        /// Gets or sets number of through passengers in the trip up until this stop.
        /// </summary>
        [FieldName("through_passengers")]
        public int? ThroughPassengers { get; set; }

        /// <summary>
        /// Gets or sets number of total passengers in the trip up until this stop.
        /// </summary>
        [FieldName("total_passengers")]
        public int? TotalPassengers { get; set; }

        /// <summary>
        /// Returns a description of this StopTime's times.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return string.Format("{0}-{1}", ArrivalTime, DepartureTime);
            //return string.Format("[{0}:{1}] {2}", this.TripId, this.StopId, this.StopHeadsign);
        }

        /// <summary>
        /// Compares this StopTime to the given object.
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public int CompareTo(object obj)
        {
            var other = (obj as StopTime);
            if (this.TripId.Equals(other.TripId))
            { // trip id's equal compare stop sequence.
                return this.StopSequence.CompareTo(other.StopSequence);
            }
            return this.TripId.CompareTo(other.TripId);
        }
        
        /// <summary>
        /// Serves as a hash function.
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            unchecked
            {
                int hash = 53;
                hash = hash * 59 + this.ArrivalTime.GetHashCode();
                hash = hash * 59 + this.DepartureTime.GetHashCode();
                hash = hash * 59 + this.DropOffType.GetHashCode();
                hash = hash * 59 + this.PickupType.GetHashCode();
                hash = hash * 59 + this.ShapeDistTravelled.GetHashCodeEmptyWhenNull();
                hash = hash * 59 + this.StopHeadsign.GetHashCodeEmptyWhenNull();
                hash = hash * 59 + this.StopId.GetHashCodeEmptyWhenNull();
                hash = hash * 59 + this.StopSequence.GetHashCode();
                hash = hash * 59 + this.TripId.GetHashCodeEmptyWhenNull();
                hash = hash * 59 + this.PassengerBoarding != null ? this.PassengerBoarding.GetHashCode() : 0;
                hash = hash * 59 + this.PassengerAlighting != null ? this.PassengerAlighting.GetHashCode() : 0;
                hash = hash * 59 + this.ThroughPassengers != null ? this.ThroughPassengers.GetHashCode() : 0;
                hash = hash * 59 + this.TotalPassengers != null ? this.TotalPassengers.GetHashCode() : 0;
                return hash;
            }
        }

        /// <summary>
        /// Returns true if the given object contains the same data.
        /// </summary>
        public override bool Equals(object obj)
        {
            var other = (obj as StopTime);
            if (other != null)
            {
                return this.ArrivalTime.Equals(other.ArrivalTime) &&
                    this.DepartureTime.Equals(other.DepartureTime) &&
                    this.DropOffType == other.DropOffType &&
                    this.PickupType == other.PickupType &&
                    (this.ShapeDistTravelled ?? string.Empty) == (other.ShapeDistTravelled ?? string.Empty) &&
                    (this.StopHeadsign ?? string.Empty) == (other.StopHeadsign ?? string.Empty) &&
                    (this.StopId ?? string.Empty) == (other.StopId ?? string.Empty) &&
                    this.StopSequence == other.StopSequence &&
                    (this.TripId ?? string.Empty) == (other.TripId ?? string.Empty) &&
                    (this.PassengerBoarding ?? 0) == (other.PassengerBoarding ?? 0) &&
                    (this.PassengerAlighting ?? 0) == (other.PassengerAlighting ?? 0) &&
                    (this.ThroughPassengers ?? 0) == (other.ThroughPassengers ?? 0) &&
                    (this.TotalPassengers ?? 0) == (other.TotalPassengers ?? 0);
            }
            return false;
        }

        /// <summary>
        /// Returns a new StopTime object created from a previous stop time object
        /// </summary>
        public static StopTime From(StopTime other)
        {
            return new StopTime()
            {
                ArrivalTime = other.ArrivalTime,
                DepartureTime = other.DepartureTime,
                DropOffType = other.DropOffType,
                PickupType = other.PickupType,
                ShapeDistTravelled = other.ShapeDistTravelled,
                StopHeadsign = other.StopHeadsign,
                StopId = other.StopId,
                StopSequence = other.StopSequence,
                Tag = other.Tag,
                TripId = other.TripId,
                PassengerBoarding = other.PassengerBoarding,
                PassengerAlighting = other.PassengerAlighting,
                ThroughPassengers = other.ThroughPassengers,
                TotalPassengers = other.TotalPassengers
            };
        }
    }
}