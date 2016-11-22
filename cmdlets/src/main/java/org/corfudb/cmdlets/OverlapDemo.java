package org.corfudb.cmdlets;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.FGMap;
import org.corfudb.runtime.collections.SMRMap;

import java.util.Map;
import java.util.Set;

/**
 * Created by dmalkhi on 11/15/16, slfritchie on 11/16/16.
 */
public class OverlapDemo {
  private static CorfuRuntime getRuntimeAndConnect(String configurationString) {
    CorfuRuntime corfuRuntime = new CorfuRuntime(configurationString).connect();
    return corfuRuntime;
  }

  /*
(qc@sfritchie-m01)57> C999.
[{[{init,{state,smrmap,"sfritchie-m01:8000",
                [cmdlet0,cmdlet1,cmdlet2,cmdlet3,cmdlet4,cmdlet5,cmdlet6,
                 cmdlet7,cmdlet8,cmdlet9],
                false,42,[]}},
   {set,{var,1},
        {call,map_qc,reset,
              [{cmdlet2,'corfu-8000@sfritchie-m01'},
               "sfritchie-m01:8000"]}},
   {set,{var,2},
        {call,map_qc,isEmpty,
              [{cmdlet7,'corfu-8000@sfritchie-m01'},
               "sfritchie-m01:8000",42,smrmap]}},
   {set,{var,3},
        {call,map_qc,clear,
              [{cmdlet0,'corfu-8000@sfritchie-m01'},
               "sfritchie-m01:8000",42,smrmap]}},
   {set,{var,4},
        {call,map_qc,entrySet,
              [{cmdlet0,'corfu-8000@sfritchie-m01'},
               "sfritchie-m01:8000",42,smrmap]}},
   {set,{var,5},
        {call,map_qc,put,
              [{cmdlet6,'corfu-8000@sfritchie-m01'},
               "sfritchie-m01:8000",42,smrmap,"b",
               "sbpbuweusuveghjlywktkhdkmmcbgxpep"]}},
   {set,{var,6},
        {call,map_qc,containsValue,
              [{cmdlet6,'corfu-8000@sfritchie-m01'},
               "sfritchie-m01:8000",42,smrmap,"gzafoyp"]}},
   {set,{var,7},
        {call,map_qc,remove,
              [{cmdlet2,'corfu-8000@sfritchie-m01'},
               "sfritchie-m01:8000",42,smrmap,"s"]}},
   {set,{var,8},
        {call,map_qc,entrySet,
              [{cmdlet9,'corfu-8000@sfritchie-m01'},
               "sfritchie-m01:8000",42,smrmap]}},
   {set,{var,9},
        {call,map_qc,remove,
              [{cmdlet8,'corfu-8000@sfritchie-m01'},
               "sfritchie-m01:8000",42,smrmap,"v"]}},
   {set,{var,10},
        {call,map_qc,entrySet,
              [{cmdlet1,'corfu-8000@sfritchie-m01'},
               "sfritchie-m01:8000",42,smrmap]}},
   {set,{var,11},
        {call,map_qc,keySet,
              [{cmdlet4,'corfu-8000@sfritchie-m01'},
               "sfritchie-m01:8000",42,smrmap]}},
   {set,{var,12},
        {call,map_qc,remove,
              [{cmdlet7,'corfu-8000@sfritchie-m01'},
               "sfritchie-m01:8000",42,smrmap,"b"]}}],
  [[{set,{var,13},
         {call,map_qc,put,
               [{cmdlet6,'corfu-8000@sfritchie-m01'},
                "sfritchie-m01:8000",42,smrmap,"a",[]]}},
    {set,{var,14},
         {call,map_qc,keySet,
               [{cmdlet7,'corfu-8000@sfritchie-m01'},
                "sfritchie-m01:8000",42,smrmap]}},
    {set,{var,15},
         {call,map_qc,put,
               [{cmdlet7,'corfu-8000@sfritchie-m01'},
                "sfritchie-m01:8000",42,smrmap,"x","Hello-world!"]}}],
   [{set,{var,16},
         {call,map_qc,put,
               [{cmdlet0,'corfu-8000@sfritchie-m01'},
                "sfritchie-m01:8000",42,smrmap,"x","Hello-world!"]}},
    {set,{var,17},
         {call,map_qc,containsKey,
               [{cmdlet1,'corfu-8000@sfritchie-m01'},
                "sfritchie-m01:8000",42,smrmap,"p"]}},
    {set,{var,18},
         {call,map_qc,size,
               [{cmdlet9,'corfu-8000@sfritchie-m01'},
                "sfritchie-m01:8000",42,smrmap]}}]]}]
   */
  public static void main(String[] args) {
    try {
      String endpoint = args[0];
      String streamName = args[1];

      CorfuRuntime runtime1 = getRuntimeAndConnect(endpoint);

      Map<String, String> m1 = runtime1.getObjectsView()
              .build()
              .setType(SMRMap.class)
              .setStreamName(streamName)
              .open();

      m1.isEmpty();
      m1.clear();
      m1.entrySet();
      m1.put("b","sbpbuweusuveghjlywktkhdkmmcbgxpep");
      m1.containsValue("gzafoyp");
      m1.remove("s");
      m1.entrySet();
      m1.remove("v");
      m1.entrySet();
      m1.keySet();
      m1.remove("b");

      Thread t2 = new Thread(() -> {

        CorfuRuntime runtime2 = getRuntimeAndConnect(endpoint);

        Map<String, String> m2 = runtime2.getObjectsView()
                .build()
                .setType(SMRMap.class)
                .setStreamName(streamName)
                .open();

        m2.put("a","");
        m2.keySet();
        m2.put("x","Hello-world!");

        cleanup(runtime2);
      });

      Thread t3 = new Thread(() -> {

        CorfuRuntime runtime3 = getRuntimeAndConnect(endpoint);

        Map<String, String> m3 = runtime3.getObjectsView()
                .build()
                .setType(SMRMap.class)
                .setStreamName(streamName)
                .open();

        m3.put("x","Hello-world!");
        m3.containsKey("p");
        m3.size();

        cleanup(runtime3);
      });

      t2.start();
      t3.start();

      try {
        t2.join();
        t3.join();
      } catch (Exception e) {
        System.out.printf("Oops, exception %s\n", e.toString());
      }

      cleanup(runtime1);
    } catch (Exception e) {
      System.out.printf("Oops, exception %s\n", e.toString());
    }
  }

  public static void cleanup(CorfuRuntime rt) {
      // Brrrrr, state needs resetting in rt's ObjectsView
      rt.getObjectsView().getObjectCache().clear();
      // Brrrrr, state needs resetting in rt's AddressSpaceView
      rt.getAddressSpaceView().resetCaches();
      // Stop the router, sortof.  false means don't really shutdown,
      // but disconnect any existing connection.
      rt.stop(false);
  }
}

