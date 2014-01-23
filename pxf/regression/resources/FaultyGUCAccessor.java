import com.pivotal.pxf.api.ReadAccessor;
import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;

public class FaultyGUCAccessor extends Plugin implements ReadAccessor {
    public FaultyGUCAccessor(InputData metaData) {
        super(metaData);
    }

    @Override
    public boolean openForRead() throws Exception {
		throw new Exception(getClass().getSimpleName() + ": login " + 
							inputData.getLogin() + " secret " + 
							inputData.getSecret());
    }

    @Override
    public OneRow readNextObject() throws Exception {
		throw new Exception("not implemented");
    }

    @Override
    public void closeForRead() throws Exception {
		throw new Exception("not implemented");
    }
}
