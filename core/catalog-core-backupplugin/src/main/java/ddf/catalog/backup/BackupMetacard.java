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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by mikev on 1/7/15.
 */
public class BackupMetacard implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(BackupMetacard.class);

    private Metacard mcToBackup;
    private File backupFile;
    private List<String> errors;

    BackupMetacard(Metacard metacard, File file, List<String> errorList) {
        mcToBackup = metacard;
        backupFile = file;
        errors = errorList;
    }

    @Override
    public void run() {
        try {
            BackupUtils.backupMetacard(mcToBackup, backupFile);
        } catch (IOException e) {
            errors.add(mcToBackup.getId());
        }
    }
}
