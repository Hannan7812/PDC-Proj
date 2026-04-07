package pdc;
import java.io.IOException;
import java.util.Scanner;

public class MultiTerminalLauncher {

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Enter number of processes: ");
        int n = scanner.nextInt();

        String command = "mvn -q exec:java -Dexec.mainClass=pdc.AppWorker";
        String command1 = "mvn -q exec:java -Dexec.mainClass=pdc.AppMaster";

         // Start the master process first
        try {            ProcessBuilder pbMaster = new ProcessBuilder(
                    "gnome-terminal",
                    "--",
                    "bash",
                    "-c",
                    command1 + "; exec bash"
            );
            pbMaster.start();
            Thread.sleep(500); // Small delay to ensure master starts before workers
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < n; i++) {
            try {
                ProcessBuilder pb = new ProcessBuilder(
                        "gnome-terminal",
                        "--",
                        "bash",
                        "-c",
                        command + "; exec bash"
                );

                pb.start();

                // Small delay to avoid overwhelming system
                Thread.sleep(30);

            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }

        scanner.close();
    }
}