package testutils;

import org.junit.runner.notification.RunNotifier;
import org.junit.runners.model.InitializationError;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

public class SpringWrappedTestRunner extends SpringJUnit4ClassRunner {

    private SpringInitializationListener initializationListener;

    public SpringWrappedTestRunner(Class<?> clazz) throws InitializationError {
        super(clazz);
    }

    @Override
    protected Object createTest() throws Exception {

        Object test = super.createTest();

        // Note that JUnit4 will call this createTest() multiple times for each
        // test method, so we need to ensure to call "beforeClassSetup" only once.
        if (test instanceof SpringInitializationListener && initializationListener == null) {
            initializationListener = (SpringInitializationListener) test;
            initializationListener.beforeClass();
        }

        return test;

    }

    @Override
    public void run(RunNotifier notifier) {
        super.run(notifier);
        if (initializationListener != null)
            initializationListener.afterClass();
    }
}
