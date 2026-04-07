package pdc;

import pdc.config.AppConfig;
import pdc.master.MasterServer;

public class AppMaster {
    public static void main(String[] args) {
        AppConfig config = AppConfig.load();
        MasterServer server = new MasterServer(config);
        server.start();
    }
}
