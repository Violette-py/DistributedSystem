package utils;

import java.io.File;
import java.util.Random;

public class FileSystem {
    public static String newFilename(){
        /* Wish the name has not appeared before in your NameNode, Good Luck _< */
        return String.valueOf((int) (Math.random() * Integer.MAX_VALUE))+"TEST";
    }
}
