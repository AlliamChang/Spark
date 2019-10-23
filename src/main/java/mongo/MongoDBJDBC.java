package mongo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class MongoDBJDBC{
    MongoClient mongoClient;
    MongoDatabase mongoDatabase;
    public MongoDBJDBC(){
        try{
            // 连接到 mongodb 服务
            mongoClient = new MongoClient( "localhost" , 27017 );

            // 连接到数据库
            mongoDatabase = mongoClient.getDatabase("twitter");


            System.out.println("Connect to database successfully");
        }catch(Exception e){
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
        }
    }

    public void readFileToDB(){
        try {
            File file = new File(this.getClass().getClassLoader().getResource("title.basics.tsv").getPath());
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            String strLine = null;
            int lineCount = 0;
            MongoCollection<Document> collection = mongoDatabase.getCollection("test");
            collection.drop();
            List<Document> documents = new ArrayList<Document>();
            bufferedReader.readLine();  // 去首行
            while(null != (strLine = bufferedReader.readLine())){
                String[] each = strLine.split("\t");
                Document document = new Document("tconst", each[0]).
                        append("primaryTitle", each[2]).
                        append("year", each[5]).
                        append("genre", each[8]);
                documents.add(document);
//                System.out.println("第[" + lineCount + "]行数据:[" + strLine + "]");
                lineCount++;
                if (lineCount%10000 == 0){
                    System.out.println(lineCount);
                }
                break;
            }
            collection.insertMany(documents);
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}