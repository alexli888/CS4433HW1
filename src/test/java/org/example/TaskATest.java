package org.example;

import org.junit.Test;

public class TaskATest {

    @Test
    public void debug() throws Exception {
        String[] input = new String[2];

        /*
        1. put the data.txt into a folder in your pc
        2. add the path for the following two files.
            windows : update the path like "file:///C:/Users/.../projectDirectory/data.txt"
            mac or linux: update the path like "file:///Users/.../projectDirectory/data.txt"
        */

        input[0] = "file:///Users/alexli/CS4433/HW 1/HW1/src/access_logs.csv";
        input[1] = "file:///Users/alexli/CS4433/HW 1/HW1/src/friends.csv";
        input[2] = "file:///Users/alexli/CS4433/HW 1/HW1/src/pages.csv";

        TaskA wc = new TaskA();
        wc.debug(input);
    }
}