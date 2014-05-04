package ClusterUtils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

final class ClustersFilter implements PathFilter {

	public boolean accept(Path path) {
		// TODO Auto-generated method stub
		
		    String pathString = path.toString();
		    return pathString.contains("/clusters-");
		
	}
	  
}