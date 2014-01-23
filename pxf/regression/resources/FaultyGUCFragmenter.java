import com.pivotal.pxf.api.Fragmenter;
import com.pivotal.pxf.api.Fragment;
import com.pivotal.pxf.api.utilities.InputData;

import java.util.List;

public class FaultyGUCFragmenter extends Fragmenter {
    public FaultyGUCFragmenter(InputData metaData) {
        super(metaData);
    }

    @Override
    public List<Fragment> getFragments() throws Exception {
		throw new Exception(getClass().getSimpleName() + ": login " + 
							inputData.getLogin() + " secret " + 
							inputData.getSecret());
    }
}
