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

import ddf.catalog.operation.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by mikev on 1/7/15.
 */
public class UpdateMetacard implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateMetacard.class);

    private Update update;
    private File fileToBackup;
    private File fileToDelete;
    private List<String> deleteErrors;
    private List<String> backupErrors;

    UpdateMetacard(Update updateDetails, File backupFile, File deleteFile, List<String> deleteErrorList, List<String> backupErrorList) {
        update = updateDetails;
        fileToBackup = backupFile;
        fileToDelete = deleteFile;
        deleteErrors = deleteErrorList;
        backupErrors = backupErrorList;
    }

    @Override public void run() {
        // Stage the old Metacard before we delete it to ensure the new Metacard is backed up
        boolean staged = false;
        try {
            BackupUtils.stageMetacard(fileToDelete);
            staged = true;
        } catch (IOException e) {
            deleteErrors.add(update.getOldMetacard().getId());
        }

        // Backup the new Metacard
        boolean backedup = false;
        try {
            BackupUtils.backupMetacard(update.getNewMetacard(), fileToBackup);
            backedup = true;
        } catch (IOException e) {
            backupErrors.add(update.getNewMetacard().getId());
        }

        // Delete the staged old Metacard if there were no errors backing up the new Metacard
        if (staged && backedup) {
            try {
                BackupUtils.deleteStagedMetacard(fileToDelete);
            } catch (IOException e) {
                deleteErrors.add(update.getOldMetacard().getId());
            }
        }
    }
}
