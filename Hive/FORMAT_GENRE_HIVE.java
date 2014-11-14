import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class FORMAT_GENRE_HIVE extends UDF {
    public Text evaluate(Text input) {
        
    	String inpt = input.toString();
		String[] genre = inpt.split("\\|");
		StringBuilder str = new StringBuilder();
		    		
		int count = 1;
		int length = genre.length;
		for(String element: genre)
		    {
		    	if(length==1){
		    		str.append(element).append(" - ID");
		    	break;
		    	}
		    	if(count==(length-1)){
		    		str.append(element+", & ");
		    		count++;
		    	continue;
		    	}
		    	if(count==length){
		    		str.append(element).append(" - ID");
		    	break;
		    	}
		    		str.append(element+", ");
		    	count++;
		   }
		return new Text(str.toString());
    }
}