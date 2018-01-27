package traindelays.networkrail.tocs

import traindelays.networkrail.scheduledata.AtocCode

package object tocs {

  val mapping = Map(
    AtocCode("AR") -> "Alliance Rail",
    AtocCode("NT") -> "Arriva Trains Northern",
    AtocCode("AW") -> "Arriva Trains Wales",
    AtocCode("CC") -> "c2c",
    AtocCode("CS") -> "Caledonian Sleeper",
    AtocCode("CH") -> "Chiltern Railway",
    AtocCode("XC") -> "CrossCountry",
    AtocCode("ZZ") -> "Devon and Cornwall Railways",
    AtocCode("EM") -> "East Midlands Trains",
    AtocCode("ES") -> "Eurostar",
    AtocCode("HT") -> "First Hull Trains",
    AtocCode("GX") -> "Gatwick Express",
    AtocCode("ZZ") -> "GB Railfreight",
    AtocCode("GN") -> "Govia Thameslink Railway (Great Northern)",
    AtocCode("TL") -> "Govia Thameslink Railway (Thameslink)",
    AtocCode("GC") -> "Grand Central",
    AtocCode("LN") -> "Great North Western Railway",
    AtocCode("GW") -> "Great Western Railway",
    AtocCode("LE") -> "Greater Anglia",
    AtocCode("HC") -> "Heathrow Connect",
    AtocCode("HX") -> "Heathrow Express",
    AtocCode("IL") -> "Island Lines",
    AtocCode("LS") -> "Locomotive Services",
    AtocCode("LM") -> "London Midland",
    AtocCode("LO") -> "London Overground",
    AtocCode("LT") -> "London Underground",
    AtocCode("ME") -> "Merseyrail",
    AtocCode("TW") -> "Nexus (Tyne & Wear Metro)",
    AtocCode("NY") -> "North Yorkshire Moors Railway",
    AtocCode("SR") -> "ScotRail",
    AtocCode("SW") -> "South Western Railway",
    AtocCode("SJ") -> "South Yorkshire Supertram",
    AtocCode("SE") -> "Southeastern",
    AtocCode("SN") -> "Southern",
    AtocCode("SP") -> "Swanage Railway",
    AtocCode("XR") -> "TfL Rail",
    AtocCode("TP") -> "TransPennine Express",
    AtocCode("VT") -> "Virgin Trains",
    AtocCode("GR") -> "Virgin Trains East Coast",
    AtocCode("WR") -> "West Coast Railway Co."
  )
}
