package pdc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

public class MultiTerminalLauncher {

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Enter number of processes: ");
        int n = scanner.nextInt();

        String workerCommand = "mvn -q exec:java -Dexec.mainClass=pdc.AppWorker";
        String masterCommand = "mvn -q exec:java -Dexec.mainClass=pdc.AppMaster";

        try {
            // 🔹 Start master WITHOUT gnome-terminal so we can read output
            ProcessBuilder pbMaster = new ProcessBuilder(
                    "bash",
                    "-c",
                    masterCommand
            );

            pbMaster.redirectErrorStream(true);
            Process masterProcess = pbMaster.start();

            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(masterProcess.getInputStream())
            );

            String line;
            boolean ready = false;

            System.out.println("Waiting for master to become ready...");

            while ((line = reader.readLine()) != null) {
                System.out.println("[MASTER] " + line);

                if (line.contains("initialized")) {
                    ready = true;
                    break;
                }
            }

            if (!ready) {
                System.out.println("Master did not signal readiness. Exiting...");
                scanner.close();
                return;
            }

            System.out.println("Master is ready. Launching workers...");

            // Launch worker processes in separate terminals
            for (int i = 0; i < n; i++) {
                ProcessBuilder pbWorker = new ProcessBuilder(
                        "gnome-terminal",
                        "--",
                        "bash",
                        "-c",
                        workerCommand + "; exec bash"
                );

                pbWorker.start();

                // Small delay to avoid system overload
                Thread.sleep(50);
            }

            while ((line = reader.readLine()) != null) {
                System.out.println("[MASTER] " + line);
            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        scanner.close();
    }
}