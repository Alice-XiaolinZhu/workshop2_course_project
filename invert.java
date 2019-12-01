import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class invert {
	private Map<String,ArrayList<String>> map = new HashMap<>();
	private ArrayList<String> list;
	
	public static void getFilename(String path,ArrayList<String>Filename) {
		File file =new File(path);
		File[] tempList =file.listFiles();
		for(int i=0; i<tempList.length;i++) {
			if(tempList[i].isFile()) {
				Filename.add(tempList[i].getName());
			}
		}
	}
	
	public void Index(String file) {
		String[] words =null;
		try {
			File filel = new File(file);
			BufferedReader reader=new BufferedReader(new FileReader(filel));
			String str =null;
			while ((str=reader.readLine())!=null) {
				words=str.split(" ");
				for (String st : words) {
					String[] s=file.split("/");
					int i=s.length;
					if (!map.containsKey(st)) {
						list=new ArrayList<String>();
						list.add(s[i-1]);
						map.put(st, list);
					}else {
						list=map.get(st);
						if (!list.contains(s[i-1])) {
							list.add(s[i-1]);
						}
					}
				}
			} 
			reader.close();
		}
			 catch(IOException e) {
				  e.printStackTrace();  
			}	
	}
	public static void main(String[] args) {
		invert ind=new invert();
		ArrayList Filename=new ArrayList<String>();
		String path ="hdfs://master-17300261262:9000/input";
		getFilename(path,Filename);
		for(int i =0;i<Filename.size();i++) {
			ind.Index(path+"/"+Filename.get(i).toString());
		}
		for (Map.Entry<String, ArrayList<String>> map : ind.map.entrySet()) {
			System.out.println(map.getKey()+":"+map.getValue());
		}	
 }
	
	
	
	
	
	
	
	
	
	
	
	
	
	

}
