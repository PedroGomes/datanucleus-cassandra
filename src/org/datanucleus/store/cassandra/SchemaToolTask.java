/**********************************************************************
Copyright (c) 2004 Andy Jefferson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors:
2004 Andy Jefferson - added sysproperty and classpath input options
2004 Andy Jefferson - changed to be derived from Java taskdef
2004 Andy Jefferson - removed redundant methods. Changed to se fork=true always
    ...
**********************************************************************/
package org.datanucleus.store.cassandra;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.taskdefs.Java;
import org.apache.tools.ant.types.FileSet;
import org.datanucleus.util.Localiser;

/**
 * SchemaTool Ant Task. Accepts the following parameters
 * <UL>
 * <LI><B>mode</B> Mode of operation (<B>"create"</B>, "delete", "validate", "dbinfo", "schemainfo").</LI>
 * <LI><B>verbose</B> Verbose output.</LI>
 * <LI><B>props</B> Name of a properties file for use in SchemaTool (PMF properties)</LI>
 * <LI><B>ddlFile</B> Name of a file to output the DDL into</LI>
 * <LI><B>completeDdl</B> Whether to output complete DDL (otherwise just the missing tables/constraints)</LI>
 * <LI><B>persistenceUnit</B> Name of a persistence-unit to manage the schema for</LI>
 * </UL>
 */
public class SchemaToolTask extends Java
{
    /** Localiser for messages. */
    private static final Localiser LOCALISER = Localiser.getInstance(
        "org.datanucleus.store.cassandra.Localisation", CassandraStoreManager.class.getClassLoader());

    /** Operating mode */
    private int runMode = SchemaTool.SCHEMATOOL_CREATE_MODE;

    /** Filesets of files (mapping or class) to be used in generating the schema. */
    List<FileSet> filesets = new ArrayList<FileSet>();

    /**
     * Constructor.
     */
    public SchemaToolTask()
    {
        setClassname("org.datanucleus.store.cassandra.SchemaTool");
        setFork(true); // Default to fork=true
    }

    /**
     * Execute method, to execute the task.
     * @throws BuildException if any error happens while running the task
     **/
    public void execute()
    throws BuildException
    {
        if (runMode == SchemaTool.SCHEMATOOL_CREATE_MODE)
        {
            createArg().setValue("-create");
        }
        else if (runMode == SchemaTool.SCHEMATOOL_DELETE_MODE)
        {
            createArg().setValue("-delete");
        }
        else if (runMode == SchemaTool.SCHEMATOOL_VALIDATE_MODE)
        {
            createArg().setValue("-validate");
        }
        else if (runMode == SchemaTool.SCHEMATOOL_DATABASE_INFO_MODE)
        {
            createArg().setValue("-dbinfo");
        }
        else if (runMode == SchemaTool.SCHEMATOOL_SCHEMA_INFO_MODE)
        {
            createArg().setValue("-schemainfo");
        }

        File[] files = getFiles();
        for (int i=0; i<files.length; i++)
        {
            createArg().setFile(files[i]);
        }

        super.execute();
    }

    /**
     * Add a fileset. @see ant manual
     * @param fs the FileSet
     */
    public void addFileSet(FileSet fs)
    {
        filesets.add(fs);
    }

    protected File[] getFiles()
    {
        List<File> v = new ArrayList<File>();

        Iterator<FileSet> filesetIter = filesets.iterator();
        while (filesetIter.hasNext())
        {
            FileSet fs = filesetIter.next();
            DirectoryScanner ds = fs.getDirectoryScanner(getProject());
            ds.scan();
            String[] f = ds.getIncludedFiles();
            for (int j = 0; j < f.length; j++)
            {
                String pathname = f[j];
                File file = new File(ds.getBasedir(), pathname);
                file = getProject().resolveFile(file.getPath());
                v.add(file);
            }
        }

        return v.toArray(new File[v.size()]);
    }

    /**
     * set verbose
     * @param verbose Whether to give verbose output
     */
    public void setVerbose(boolean verbose)
    {
        if (verbose)
        {
            createArg().setValue("-v");
            log("SchemaTool verbose: " + verbose, Project.MSG_VERBOSE);
        }
    }

    /**
     * Get persistence properties from a file
     * @param propsFileName Name of props file
     */
    public void setProps(String propsFileName)
    {
        if (propsFileName != null && propsFileName.length() > 0)
        {
            createArg().setLine("-props " + propsFileName);
            log("SchemaTool props: " + propsFileName, Project.MSG_VERBOSE);
        }
    }

    /**
     * Set the file to output DDL to
     * @param file Name of DDL file
     */
    public void setDdlFile(String file)
    {
        if (file != null && file.length() > 0)
        {
            createArg().setLine("-ddlFile " + file);
            log("SchemaTool ddlFile: " + file, Project.MSG_VERBOSE);
        }
    }

    /**
     * Mutator for whether to output complete DDL.
     * @param complete Whether to give complete DDL
     */
    public void setCompleteDdl(boolean complete)
    {
        if (complete)
        {
            createArg().setValue("-completeDdl");
            log("SchemaTool completeDdl: " + complete, Project.MSG_VERBOSE);
        }
    }

    /**
     * Set the name of the persistence unit to manage
     * @param unitName Name of persistence-unit
     */
    public void setPersistenceUnit(String unitName)
    {
        if (unitName != null && unitName.length() > 0)
        {
            createArg().setLine("-pu " + unitName);
            log("SchemaTool pu: " + unitName, Project.MSG_VERBOSE);
        }
    }

    /**
     * Set the API Adapter
     * @param api API Adapter
     */
    public void setApi(String api)
    {
        if (api != null && api.length() > 0)
        {
            createArg().setValue("-api");
            createArg().setValue(api);
            log("SchemaTool api: " + api, Project.MSG_VERBOSE);
        }
    }

    /**
     * Sets the mode of operation.
     * @param mode The mode of operation ("create", "delete", "validate", "dbinfo", "schemainfo")
     */
    public void setMode(String mode)
    {
        if (mode == null)
        {
            return;
        }
        if (mode.equalsIgnoreCase("create"))
        {
            this.runMode = SchemaTool.SCHEMATOOL_CREATE_MODE;
        }
        else if (mode.equalsIgnoreCase("delete"))
        {
            this.runMode = SchemaTool.SCHEMATOOL_DELETE_MODE;
        }
        else if (mode.equalsIgnoreCase("validate"))
        {
            this.runMode = SchemaTool.SCHEMATOOL_VALIDATE_MODE;
        }
        else if (mode.equalsIgnoreCase("dbinfo"))
        {
            this.runMode = SchemaTool.SCHEMATOOL_DATABASE_INFO_MODE;
        }        
        else if (mode.equalsIgnoreCase("schemainfo"))
        {
            this.runMode = SchemaTool.SCHEMATOOL_SCHEMA_INFO_MODE;
        }        
        else
        {
            System.err.println(LOCALISER.msg("014036"));
        }
    }
}