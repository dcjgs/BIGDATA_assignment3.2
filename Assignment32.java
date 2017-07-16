import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class Assignment32 {
	
	private static void listfilesTask1(Path path,FileSystem fs, long start_ts, long end_ts) throws FileNotFoundException, IOException {
		// TODO Auto-generated method stub
		
		List<String> fileList = new ArrayList<String>();
		FileStatus[] status = fs.listStatus(path);
		
		for (FileStatus stat : status) {
			
			System.out.println("==> stat.getModificationTime()="+stat.getModificationTime()+" start_ts="+start_ts+", end_ts="+end_ts);
			if (stat.getModificationTime() > start_ts && stat.getModificationTime() < end_ts  )
			{
				System.out.println("Name = " + stat.getOwner());
				System.out.println("Path = " + stat.getPath().toString());
				System.out.println("Isfile = " + stat.isFile());
				System.out.println("IsDir = " + stat.isDir());
			}
		}
		
		
	}
	
	private static List<String> listfilesTask2(Path path,FileSystem fs) throws FileNotFoundException, IOException {
		// TODO Auto-generated method stub
		
		List<String> fileList = new ArrayList<String>();
		FileStatus[] status = fs.listStatus(path);
		
		for (FileStatus stat : status) {
			
			
			fileList.add(stat.getPath().toString());
			
			if (stat.isDir())
			{			    
				List<String> deeperList = listfilesTask2(stat.getPath(), fs);
				fileList.addAll(deeperList);
			}
			
			    
		}
		return fileList;
		
	}
	
	private static void displayDetails(String[] arguments, FileSystem fs) throws FileNotFoundException, IOException {
		
		for (int a=1; a< arguments.length; a++){
			System.out.println("===> Recursively list directories - Assignment 3.1 Task3..Now doing path "+arguments[a]+" <====");
			List<String> newlist = listfilesTask2(new Path(arguments[a]), fs);
			for (int i = 0; i < newlist.size(); i++) {
				System.out.println(newlist.get(i));
			}
		}
		
	}

	
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
		Path path= new Path("/acadguild");
		long start_ts=0;
		long end_ts=0;
		try {
			FileSystem fs = FileSystem.get(conf);
			
			if (args.length == 0 ){
				System.out.println("************* displaying file/dir details in hdfs for default path /acadguild **********");
				System.out.println("Usage1 Displaying file/dir details between timestamp range in hdfs : hadoop jar assignment32.jar ");
				System.out.println("Usage2 Task4 Displaying file/dir details between timestamp range in hdfs : hadoop jar assignment32.jar task4 /acadguild start_ts end_ts");
				System.out.println("Usage3 Task5 Display hdfs file - Assignment 3.2 Task5 : hadoop jar assignment32.jar task5 /acadguild/testDir/samples/details.txt"); 
				System.out.println("Usage4 Task6 Copy a file from local filesystem to HDFS Assignment assignment3.jar task6 /acadguild/testDir/samples/details.txt /acadguild/testDir");
				//path = new Path("/acadguild");
				System.out.println("==> list directories - Assignment 3.2 (default TS values 0 , 864000000000000L )");
				listfilesTask1(path, fs ,0,864000000000000L);

				
			} 
			// Usage2 Task4 displaying file/dir details between timestamp range in hdfs : hadoop jar assignment32.jar task4 /acadguild start_ts end_ts
			if (args.length == 4 && "task4".equalsIgnoreCase(args[0])) {
				System.out.println("************* displaying file/dir details between timestamp range in hdfs for input path "+args[1]+", start_ts,end_ts  **********");
				path = new Path(args[1]);
				if ("".equalsIgnoreCase(args[2]))
					start_ts=0;
				else 
					start_ts = Long.parseLong(args[2]);
				
				if ("".equalsIgnoreCase(args[3]))
					end_ts=864000000000000L;
				else 
					end_ts = Long.parseLong(args[3]);
				System.out.println("************* displaying start_ts="+start_ts);
				System.out.println("************* displaying end_ts="+end_ts);
				
				System.out.println("==> list files/directories - Assignment 3.2 Task4 between start_ts,end_ts");
				listfilesTask1(path, fs ,start_ts,end_ts);
			}
			
			// Usage3 Display hdfs file - Assignment 3.2 Task5 : hadoop jar assignment32.jar task5 /acadguild/testDir/samples/details.txt
			if (args.length == 2 && "task5".equalsIgnoreCase(args[0])) {
				System.out.println("==> Display hdfs file - Assignment 3.2 Task5");
				path = new Path(args[1]);	
				displayFileInHdfs(fs,args[1]);
			}
			//Usage4 Task6 copy a file from local filesystem to HDFS Assignment assignment3.jar task6 /home/hduser/details.txt /acadguild/testDir
			if (args.length > 2  && "task6".equalsIgnoreCase(args[0])) {
				System.out.println("==> FileCopy - copy a file from local filesystem to HDFS Assignment 3.2 Task6");
				copyToHDFS(args[1],args[2],fs);
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void copyToHDFS(String src, String dst, FileSystem fs) throws IllegalArgumentException, IOException {
		// TODO Auto-generated method stub
		fs.copyFromLocalFile(new Path(src), new Path(dst));
	}

	private static void displayFileInHdfs(FileSystem fs, String inFile) throws IOException {
		// TODO Auto-generated method stub
		Path inFilePath = new Path(inFile);
		//check if file exists
		//create a buffered reader using FSDI
		//use sop for displaying to screen
		
		if (!fs.exists(inFilePath)) {
			System.out.println("File does not exist in hdfs = "+inFile);
			System.exit(0);
		}
		if (!fs.isFile(inFilePath)){
			System.out.println("File not valid in hdfs="+inFile);
			System.exit(0);
		}
		System.out.println("===================== Now Reading file from HDFS===========");
		FSDataInputStream fsdi = fs.open(inFilePath);
		BufferedReader br = new BufferedReader(new InputStreamReader(fsdi));
		String line;
		line = br.readLine();
		while (line != null)
		{
			System.out.println(line);
			line = br.readLine();
		}
			
		fsdi.close();
		br.close();
		
	}

	
}
