/**
 * Copyright (c) Codice Foundation
 *
 * This is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
 * General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details. A copy of the GNU Lesser General Public License
 * is distributed along with this program and can be found at
 * <http://www.gnu.org/licenses/lgpl.html>.
 *
 **/
package ddf.catalog.backup;

import ddf.catalog.data.Metacard;
import ddf.catalog.data.impl.MetacardImpl;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * Created by mikev on 1/7/15.
 */
public class BackupUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(BackupUtils.class);

    static final String TEMP_FILE_EXTENSION = ".tmp";
    static final String DELETE_STAGE_FILE_EXTENSION = ".del";

    /**
     * Moves a backed up metacard file to a staged file in preparation for deletion
     *
     * @param metacardFile the metacard file to be staged.
     *
     * @returns True if the staging (rename) succeed; false otherwise;
     *
     * @throws java.io.IOException
     */
    static void stageMetacard(File metacardFile) throws IOException {
        File stageFile = getStageFile(metacardFile);
        FileUtils.moveFile(metacardFile, stageFile);
    }

    /**
     * Removes the staged backup file from the file system.
     *
     * @param metacardFile The staged metacard file to be removed from the file system.
     *
     * @throws java.io.IOException
     */
    static void deleteStagedMetacard(File metacardFile) throws IOException {
        File stageFile = getStageFile(metacardFile);
        FileUtils.forceDelete(stageFile);
    }

    /**
     * Backs up metacard to file system backup.
     *
     * @param metacard the metacard to be backed up
     *
     * @throws java.io.IOException
     */
    static void backupMetacard(Metacard metacard, File backupFile) throws IOException {
        // Write metacard to a temp file.  When write is complete, rename (remove temp extension).
        File tempFile = getTempFile(backupFile);
        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(tempFile));
        LOGGER.debug("Writing temp metacard [{}] to [{}].", tempFile.getName(), tempFile.getParent());
        oos.writeObject(new MetacardImpl(metacard));
        oos.close();
        BackupUtils.removeTempExtension(tempFile);
    }

    /**
     * Deletes metacard from file system backup.
     *
     * @param metacardFile the metacard file to be deleted.
     *
     * @throws java.io.IOException
     */
    static void deleteMetacard(File metacardFile) throws IOException {

        FileUtils.forceDelete(metacardFile);
    }

    /**
     * While metacards are being deleted from the backup, they are moved to staged files.
     * This makes it easy to find and remove deleted files.
     *
     * @param file the file to create a staged file for.
     * @return
     * @throws java.io.IOException
     */
    static File getStageFile(File file) throws IOException {
        return new File(file.getAbsolutePath() + DELETE_STAGE_FILE_EXTENSION);
    }

    /**
     * While metacards are being written to the file system for backup, they are written to temp files. Each temp file is renamed
     * when the write is complete. This makes it easy to find and remove failed files.
     *
     * @param file the file to create a temp file for.
     * @return
     * @throws java.io.IOException
     */
    static File getTempFile(File file) throws IOException {
        String filePath = file.getAbsolutePath();
        String tempFilePath = filePath + TEMP_FILE_EXTENSION;
        return new File(tempFilePath);
    }

    /**
     * Removed the temp file extension off of the backed up metacard.
     *
     * @param source the backed up metacard file in which the temp file extension is to be removed.
     * @throws java.io.IOException
     */
    static void removeTempExtension(File source) throws IOException {
        LOGGER.debug("Removing {} file extension.", TEMP_FILE_EXTENSION);
        if (StringUtils.endsWith(source.getAbsolutePath(), TEMP_FILE_EXTENSION)) {
            File destination = new File(StringUtils.removeEnd(source.getAbsolutePath(), TEMP_FILE_EXTENSION));
            FileUtils.moveFile(source, destination);
            LOGGER.debug("Moved {} to {}.", source.getAbsolutePath(), destination.getAbsolutePath());
        } else {
            LOGGER.debug("{} was not moved due to it not being a temp file.", source.getAbsolutePath());
        }
    }
}
