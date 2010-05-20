package tests;
//
//import javax.jdo.PersistenceManager;
//import javax.jdo.Transaction;
//
//import org.jpox.samples.models.company.Person;
//
///**
// * Application identity persistence tests for XML datastores.
// */
//public class ApplicationIdPersistenceTest extends JDOPersistenceTestCase
//{
//    Object id;
//
//    public ApplicationIdPersistenceTest(String name)
//    {
//        super(name);
//    }
//
//    public void testInsert() throws Exception
//    {
//        try
//        {
//            PersistenceManager pm = pmf.getPersistenceManager();
//            Transaction tx = pm.currentTransaction();
//            try
//            {
//                tx.begin();
//                Person p = new Person();
//                p.setPersonNum(1);
//                p.setGlobalNum("1");
//                p.setFirstName("Bugs");
//                p.setLastName("Bunny");
//
//                Person p2 = new Person();
//                p2.setPersonNum(2);
//                p2.setGlobalNum("2");
//                p2.setFirstName("My");
//                p2.setLastName("Friend");
//
//                p.setBestFriend(p2);
//
//                pm.makePersistent(p);
//                tx.commit();
//            }
//            catch (Exception e)
//            {
//                e.printStackTrace();
//                fail("Exception thrown when running test " + e.getMessage());
//            }
//            finally
//            {
//                if (tx.isActive())
//                {
//                    tx.rollback();
//                }
//                pm.close();
//            }
//        }
//        finally
//        {
//            clean(Person.class);
//        }
//    }
//
//    /**
//     * Test of persistence of more than 1 app id objects with the same "id".
//     */
//    public void testPersistDuplicates()
//    {
//        try
//        {
//            // Persist an object with id "101"
//            PersistenceManager pm = pmf.getPersistenceManager();
//            Transaction tx = pm.currentTransaction();
//            try
//            {
//                tx.begin();
//
//                Person p1 = new Person(101, "Bugs", "Bunny", "bugs.bunny@warnerbros.com");
//                p1.setGlobalNum("101");
//                pm.makePersistent(p1);
//
//                tx.commit();
//            }
//            catch (Exception e)
//            {
//                e.printStackTrace();
//                fail("Exception thrown persisting data " + e.getMessage());
//            }
//            finally
//            {
//                if (tx.isActive())
//                {
//                    tx.rollback();
//                }
//                pm.close();
//            }
//
//            pm = pmf.getPersistenceManager();
//            tx = pm.currentTransaction();
//            try
//            {
//                tx.begin();
//
//                Person p2 = new Person(101, "Bugs", "Bunny", "bugs.bunny@warnerbros.com");
//                p2.setGlobalNum("101");
//                pm.makePersistent(p2);
//
//                tx.commit();
//                fail("Was allowed to persist two application-identity objects with the same identity");
//            }
//            catch (Exception e)
//            {
//                // Expected
//            }
//            finally
//            {
//                if (tx.isActive())
//                {
//                    tx.rollback();
//                }
//                pm.close();
//            }
//        }
//        finally
//        {
//            // Do clean up
//            clean(Person.class);
//        }
//    }
//
//}