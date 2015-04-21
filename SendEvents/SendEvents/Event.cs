// Copyright (c) Microsoft Corporation. All rights reserved. See License.txt in the project root for license information.

using System.Runtime.Serialization;

namespace SendEvents
{
    public class Event
    {
        public string Id { get; set; }
        public double Lat { get; set; }
        public double Lng { get; set; }
        public long Time { get; set; }
        public string Code { get; set; }
    }
}
