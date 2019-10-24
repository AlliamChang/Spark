package mongo;

public class Main {
    public static void main(String[] args){
        MongoDBJDBC mongoDBJDBC = new MongoDBJDBC();
        mongoDBJDBC.readFileToDB();
    }
}
