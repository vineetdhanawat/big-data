import org.apache.commons.lang.ArrayUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public class FORMAT_GENRE_PIG extends EvalFunc <String> {
	@Override
	public String exec(Tuple input){
		try {
			if (input == null || input.size() == 0) {
				return null;
			}
			String inpt = (String) input.get(0);
			String[] genre = inpt.split("\\|");
			StringBuilder str = new StringBuilder();

			ArrayUtils.reverse(genre);
			
			int count = 1;
			
			int length = genre.length;
			for(String element: genre) {
				if(length==1){
					str.append(element).append(" ID");
					break;
				}
				if(count==(length-1)){
					str.append(element+" & ");
					count++;
					continue;
				}
				if(count==length){
					str.append(element).append(" ID");
					break;
				}
				str.append(element+", ");
				count++;
			}
			return str.toString();

		} catch (ExecException ex) {
			System.out.println("Error: " + ex.toString());
		}
	return null;
	}
}