package org.corfudb.cmdlets;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.FGMap;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.view.Layout;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Exchanger;
import org.codehaus.plexus.util.ExceptionUtils;

/**
 * Created by dmalkhi on 11/15/16, slfritchie on 11/16/16.
 */
public class WeirdReplex {
    private static CorfuRuntime getRuntimeAndConnect(String configurationString) {
        CorfuRuntime corfuRuntime = new CorfuRuntime(configurationString).connect();
        return corfuRuntime;
    }

    public static void main(String[] args) {
        try {
            String endpoint = args[0];
            String streamName = args[1];
            String layoutType = args[2];

            CorfuRuntime rtSetup = getRuntimeAndConnect(endpoint);
            String jsonReplex = "{\"layoutServers\":[\"" + endpoint + "\"],\"sequencers\":[\"" + endpoint + "\"],\"segments\":[{\"replicationMode\":\"REPLEX\",           \"start\":0,\"end\":-1,\"stripes\":[{\"logServers\":[\"" + endpoint + "\"]}],\"replexes\":[{\"logServers\":[\"" + endpoint + "\"]}]}],\"epoch\":3}";
            String jsonCR     = "{\"layoutServers\":[\"" + endpoint + "\"],\"sequencers\":[\"" + endpoint + "\"],\"segments\":[{\"replicationMode\":\"CHAIN_REPLICATION\",\"start\":0,\"end\":-1,\"stripes\":[{\"logServers\":[\"" + endpoint + "\"]}]                                                       }],\"epoch\":3}";
            String json;
            if (layoutType.contentEquals("replex")) {
                layoutType = "replex";
                json = jsonReplex;
            } else {
                layoutType = "chain_replication";
                json = jsonCR;
            }
            System.out.printf("JSON for our new epoch:\n%s\n", json);
            Layout replexLayout = Layout.fromJSONString(json);
            try {
                replexLayout.setRuntime(rtSetup);
                replexLayout.moveServersToEpoch();
                rtSetup.getLayoutView().committed(3L, replexLayout);
                rtSetup.invalidateLayout();
                System.out.printf("rtSetup getLayout: %s\n", rtSetup.getLayoutView().getLayout().toString());
                System.out.printf("rtSetup getCurrentLayout: %s\n", rtSetup.getLayoutView().getCurrentLayout().toString());
            } catch (Exception e) {
                System.err.printf("Bummer: updateLayout: %s\n", e.toString());
                System.err.printf("Bummer: updateLayout: %s\n", ExceptionUtils.getStackTrace(e).toString());
                System.exit(1);
            }

            CorfuRuntime runtime6 = getRuntimeAndConnect(endpoint);
            Map<String,String> testMap6 = runtime6.getObjectsView().build().setType(SMRMap.class).setStreamName(streamName).open();
            String var3 = testMap6.put("b","lumxejgbpxbdcclmmngolaq");

            String var4 = testMap6.put("v","Hello-world!");

            CorfuRuntime runtime2 = getRuntimeAndConnect(endpoint);
            Map<String,String> testMap2 = runtime2.getObjectsView().build().setType(SMRMap.class).setStreamName(streamName).open();
            String var5 = testMap2.put("a","qqshepqitpalyktjcscmjxvsqe");

            CorfuRuntime runtime8 = getRuntimeAndConnect(endpoint);
            Map<String,String> testMap8 = runtime8.getObjectsView().build().setType(SMRMap.class).setStreamName(streamName).open();
            String var6 = testMap8.put("b","");

            CorfuRuntime runtime4 = getRuntimeAndConnect(endpoint);
            Map<String,String> testMap4 = runtime4.getObjectsView().build().setType(SMRMap.class).setStreamName(streamName).open();
            String var8 = testMap4.put("c","");

            String var15 = testMap2.put("y","");

            // created for layout setup: CorfuRuntime runtime5 = getRuntimeAndConnect(endpoint);
            CorfuRuntime runtime5 = getRuntimeAndConnect(endpoint);
            Map<String,String> testMap5 = runtime5.getObjectsView().build().setType(SMRMap.class).setStreamName(streamName).open();
            String var16 = testMap5.put("s","");

            CorfuRuntime runtime9 = getRuntimeAndConnect(endpoint);
            Map<String,String> testMap9 = runtime9.getObjectsView().build().setType(SMRMap.class).setStreamName(streamName).open();
            String var18 = testMap9.put("a","qmsknkhkmmlsodtbpvkzv");

            CorfuRuntime runtime3 = getRuntimeAndConnect(endpoint);
            Map<String,String> testMap3 = runtime3.getObjectsView().build().setType(SMRMap.class).setStreamName(streamName).open();
            String var19 = testMap3.put("b","Hello-world!");

            String var20 = testMap9.put("f","Another-value");

            String var21 = testMap9.put("z","Hello-world!");

            testMap8.clear();

            CorfuRuntime runtime0 = getRuntimeAndConnect(endpoint);
            Map<String,String> testMap0 = runtime0.getObjectsView().build().setType(SMRMap.class).setStreamName(streamName).open();
            String var23 = testMap0.put("f","");

            String var27 = testMap8.put("o","Another-value");

            CorfuRuntime runtime1 = getRuntimeAndConnect(endpoint);
            Map<String,String> testMap1 = runtime1.getObjectsView().build().setType(SMRMap.class).setStreamName(streamName).open();
            String var28 = testMap1.remove("f");

            String var29 = testMap5.put("a","Hello-world!");

            System.err.printf("runtime4 getCurrentLayout: %s\n", runtime4.getLayoutView().getCurrentLayout().toString());
            String var30 = testMap4.remove("a");

            System.out.printf("value of var30: %s\n", var30);
            if (var30.equals("Hello-world!")) {
                System.out.printf("Correct value found using type %s\n", layoutType);
                System.exit(0);
            } else {
                System.out.printf("Stale value found using type %s, error!\n", layoutType);
                System.exit(1);
            }
        } catch (Exception e) {
            System.out.printf("Oops, exception %s\n", e.toString());
        }
    }
}

