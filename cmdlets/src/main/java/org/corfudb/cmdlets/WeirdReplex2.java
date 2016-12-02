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
public class WeirdReplex2 {
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

            String var5 = testMap6.put("a","qqshepqitpalyktjcscmjxvsqe");

            String var6 = testMap6.put("b","");

            String var8 = testMap6.put("c","");

            String var15 = testMap6.put("y","");

            String var16 = testMap6.put("s","");

            String var18 = testMap6.put("a","qmsknkhkmmlsodtbpvkzv");

            String var19 = testMap6.put("b","Hello-world!");

            String var20 = testMap6.put("f","Another-value");

            String var21 = testMap6.put("z","Hello-world!");

            testMap6.clear();

            String var23 = testMap6.put("f","");

            String var27 = testMap6.put("o","Another-value");

            String var28 = testMap6.remove("f");

            String var29 = testMap6.put("a","Hello-world!");

            System.err.printf("runtime6 getCurrentLayout: %s\n", runtime6.getLayoutView().getCurrentLayout().toString());
            String var30 = testMap6.remove("a");

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

