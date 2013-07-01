import java.net.InetAddress;

import com.pivotal.pxf.utilities.InputData;
import com.pivotal.pxf.fragmenters.Fragmenter;
import com.pivotal.pxf.fragmenters.FragmentsOutput;

/*
 * Class that defines the splitting of a data resource into fragments that can
 * be processed in parallel
 * * GetFragments returns the fragments information of a given path (source name and location of each fragment).
 * Used to get fragments of data that could be read in parallel from the different segments.
 * Dummy implementation, for documentation
 */
public class DummyFragmenter extends Fragmenter
{
	public DummyFragmenter(InputData metaData)
	{
		super(metaData);
	}
	
	/*
	 * path is a data source URI that can appear as a file name, a directory name  or a wildcard
	 * returns the data fragments - identifiers of data and a list of availble hosts
	 */
	public FragmentsOutput GetFragments() throws Exception
    {
        String localhostname = java.net.InetAddress.getLocalHost().getHostName();
        fragments.addFragment(this.inputData.path() + ".1" /* source name */,
                                new String[]{localhostname,localhostname} /* available hosts list */);
        fragments.addFragment(this.inputData.path() + ".2" /* source name */,
                                new String[]{localhostname,localhostname} /* available hosts list */);
        fragments.addFragment(this.inputData.path() + ".3" /* source name */,
                                new String[]{localhostname,localhostname} /* available hosts list */);
        return fragments;
    }
	
}
