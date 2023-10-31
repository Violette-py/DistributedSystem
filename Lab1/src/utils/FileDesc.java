package utils;

//TODO: According to your design, complete the FileDesc class, which wraps the information returned by NameNode open()
public class FileDesc {
    /* the id should be assigned uniquely during the lifetime of NameNode,
     * so that NameNode can know which client's open has over at close
     * e.g., on nameNode1
     * client1 opened file "Hello.txt" with mode 'w' , and retrieved a FileDesc with 0x889
     * client2 tries opening the same file "Hello.txt" with mode 'w' , and since the 0x889 is not closed yet, the return
     * value of open() is null.
     * after a while client1 call close() with the FileDesc of id 0x889.
     * client2 tries again and get a new FileDesc with a new id 0x88a
     */
    private final long id;

    public FileDesc(long id) {
        this.id = id;
    }


    /* The following method is for conversion, so we can have interface that return string, which is easy to write in idl */
//    @Override
//    public String toString() {
//        return null;
//    }
//
//    public static FileDesc fromString(String str){
//        return null;
//    }

    @Override
    public String toString() {
        return Long.toString(id);
    }

    public static FileDesc fromString(String str) {
        if (str == null) {
            throw new IllegalArgumentException("Invalid input string: null");
        }

        long id;
        try {
            id = Long.parseLong(str);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid input string: " + str);
        }

        return new FileDesc(id);
    }

    public long getId() {
        return id;
    }

}