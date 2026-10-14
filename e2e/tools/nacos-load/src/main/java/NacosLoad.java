import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Registers {@code services} x {@code instances} ephemeral instances in Nacos and holds them until the
 * process is killed, which drops the gRPC connection and so removes every instance at once.
 *
 * <pre>java -jar nacos-load.jar &lt;serverAddr&gt; &lt;servicePrefix&gt; &lt;services&gt; &lt;instances&gt;</pre>
 * Credentials come from NACOS_USERNAME / NACOS_PASSWORD. Instance i of service s is 10.10.s.i:20000+i.
 */
public class NacosLoad {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("usage: NacosLoad <serverAddr> <servicePrefix> <services> <instances>");
            System.exit(2);
        }
        String prefix = args[1];
        int services = Integer.parseInt(args[2]);
        int instances = Integer.parseInt(args[3]);

        Properties properties = new Properties();
        properties.setProperty(PropertyKeyConst.SERVER_ADDR, args[0]);
        properties.setProperty(PropertyKeyConst.USERNAME, System.getenv().getOrDefault("NACOS_USERNAME", "nacos"));
        properties.setProperty(PropertyKeyConst.PASSWORD, System.getenv().getOrDefault("NACOS_PASSWORD", ""));
        NamingService naming = NamingFactory.createNamingService(properties);

        for (int s = 0; s < services; s++) {
            List<Instance> batch = new ArrayList<>();
            for (int i = 0; i < instances; i++) {
                Instance instance = new Instance();
                instance.setIp("10.10." + s + "." + (i + 1));
                instance.setPort(20000 + i);
                instance.setMetadata(Map.of("e2e-load", prefix));
                batch.add(instance);
            }
            naming.batchRegisterInstance(prefix + "-" + s, "DEFAULT_GROUP", batch);
        }
        System.out.println("REGISTERED " + services + "x" + instances);
        Thread.currentThread().join();
    }
}
