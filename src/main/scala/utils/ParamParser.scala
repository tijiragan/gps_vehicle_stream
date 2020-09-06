package com.VehicleStream.utils
import com.google.gson.Gson

class ParamParser {
def parseRunparm(paramsString:String): java.util.Map[String,String] ={
  val gson = new Gson()

  println(paramsString)
  val params = gson.fromJson(paramsString,classOf[java.util.Map[String,String]])
  return params
}
}
