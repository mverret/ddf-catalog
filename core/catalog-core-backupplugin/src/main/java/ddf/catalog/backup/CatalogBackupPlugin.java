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
import ddf.catalog.operation.CreateResponse;
import ddf.catalog.operation.DeleteResponse;
import ddf.catalog.operation.Update;
import ddf.catalog.operation.UpdateResponse;
import ddf.catalog.plugin.PluginExecutionException;
import ddf.catalog.plugin.PostIngestPlugin;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The CatalogBackupPlugin backups up metacards to the file system.  It is a PostIngestPlugin, so it processes
 * CreateResponses, DeleteResponses, and UpdateResponses.
 *
 * The root backup directory and subdirectory levels can be configured in the Backup Post-Ingest Plugin section in the
 * admin console.
 *
 * This feature can be installed/uninstalled with the following commands:
 *
 * ddf@local>features:install catalog-core-backupplugin
 * ddf@local>features:uninstall catalog-core-backupplugin
 */

public class CatalogBackupPlugin implements PostIngestPlugin {

    private static final Logger LOGGER = LoggerFactory.getLogger(CatalogBackupPlugin.class);

    private static final int DEFAULT_THREAD_COUNT = 1000;

    private File rootBackupDir;
    private int subDirLevels;
    private int numThreads;
    ExecutorService threadPool;

    private enum OPERATION {
        CREATE,
        DELETE
    };

    public CatalogBackupPlugin() {
        rootBackupDir = null;
        subDirLevels = 0;
        numThreads = DEFAULT_THREAD_COUNT;
        threadPool = Executors.newFixedThreadPool(numThreads);
    }

    /**
     * Backs up created metacards to the file system backup.
     *
     * @param input
     *            the {@link CreateResponse} to process
     * @return
     * @throws PluginExecutionException
     */
    @Override
    public CreateResponse process(CreateResponse input) throws PluginExecutionException {
        LOGGER.debug("Performing backup of metacards in CreateResponse.");

        if (rootBackupDir == null) {
            throw new PluginExecutionException("No root backup directory configured.");
        }

        List<String> errors = Collections.synchronizedList(new ArrayList<String>());

        List<Metacard> metacards = input.getCreatedMetacards();

        for (Metacard metacard : metacards) {
            try {
                File metacardBackupFile = getBackupFile(metacard, rootBackupDir, subDirLevels);
                threadPool.execute(new BackupMetacard(metacard, metacardBackupFile, errors));
            } catch (IOException e) {
                errors.add(metacard.getId());
            }
        }

        if (errors.size() > 0) {
            throw new PluginExecutionException(getExceptionMessage(CreateResponse.class.getSimpleName(), null, errors, OPERATION.CREATE));
        }

        return input;
    }

    /**
     * Backs up updated metacards to the file system backup.
     *
     * @param input
     *            the {@link UpdateResponse} to process
     * @return
     * @throws PluginExecutionException
     */
    @Override
    public UpdateResponse process(UpdateResponse input) throws PluginExecutionException {
        LOGGER.debug("Updating metacards contained in UpdateResponse in backup.");

        if (rootBackupDir == null) {
            throw new PluginExecutionException("No root backup directory configured.");
        }

        List<String> deleteErrors = Collections.synchronizedList(new ArrayList<String>());
        List<String> backupErrors = Collections.synchronizedList(new ArrayList<String>());

        List<Update> updates = input.getUpdatedMetacards();

        String exceptionMessage = null;

        for (Update update : updates) {
            File backupFile = null;
            try {
                backupFile = getBackupFile(update.getNewMetacard(), rootBackupDir, subDirLevels);
            } catch (IOException e) {
                backupErrors.add(update.getNewMetacard().getId());
            }

            File deleteFile = null;
            try {
                deleteFile = getBackupFile(update.getOldMetacard(), rootBackupDir, subDirLevels);
                if(!deleteFile.exists()) {
                    deleteFile = null;
                    deleteErrors.add(update.getOldMetacard().getId());
                }
            } catch (IOException e) {
                deleteErrors.add(update.getOldMetacard().getId());
            }

            if (null != backupFile && null != deleteFile) {
                threadPool.execute(new UpdateMetacard(update, backupFile, deleteFile, deleteErrors, backupErrors));
            }
        }

        if (deleteErrors.size() > 0) {
            exceptionMessage = getExceptionMessage(UpdateResponse.class.getSimpleName(), exceptionMessage, deleteErrors, OPERATION.DELETE);
        }

        if (backupErrors.size() > 0) {
            exceptionMessage = getExceptionMessage(UpdateResponse.class.getSimpleName(), exceptionMessage, backupErrors, OPERATION.CREATE);
        }

        if (deleteErrors.size() > 0 || backupErrors.size() > 0) {
            throw new PluginExecutionException(exceptionMessage);
        }

        return input;
    }

    /**
     * Removes deleted metacards from the file system backup.
     * @param input
     *            the {@link DeleteResponse} to process
     * @return
     * @throws PluginExecutionException
     */
    @Override
    public DeleteResponse process(DeleteResponse input) throws PluginExecutionException {
        LOGGER.debug("Deleting metacards contained in DeleteResponse from backup.");

        if (rootBackupDir == null) {
            throw new PluginExecutionException("No root backup directory configured.");
        }

        List<String> errors = Collections.synchronizedList(new ArrayList<String>());

        List<Metacard> metacards = input.getDeletedMetacards();

        for (Metacard metacard : metacards) {
            try {
                File metacardToDelete = getBackupFile(metacard, rootBackupDir, subDirLevels);
                threadPool.execute(new DeleteMetacard(metacard, metacardToDelete, errors));
            } catch (IOException e) {
                errors.add(metacard.getId());
            }
        }

        if (errors.size() > 0) {
            throw new PluginExecutionException(getExceptionMessage(DeleteResponse.class.getSimpleName(), null, errors, OPERATION.DELETE));
        }

        return input;
    }

    /**
     * Sets the root file system backup directory.
     *
     * @param dir absolute path for the root file system backup directory.
     */
    public void setRootBackupDir(String dir) {
        if (StringUtils.isBlank(dir)) {
            LOGGER.error("The root backup directory is blank.");
            return;
        }

        this.rootBackupDir = new File(dir);
        LOGGER.debug("Set root backup directory to: {}", this.rootBackupDir.toString());
    }

    /**
     * Sets the number of subdirectory levels to create. Two characters from each metacard ID will be used to name
     * each subdirectory level.
     *
     * @param levels number of subdirectory levels to create
     */
    public void setSubDirLevels(int levels) {
        this.subDirLevels = levels;
        LOGGER.debug("Set subdirectory levels to: {}", this.subDirLevels);
    }

    /**
     * Sets the number of threads that should be used for processing.
     *
     * @param threads Number of threads to use
     */
    public void setNumThreads(int threads) {
        this.numThreads = threads;
        LOGGER.debug("Set number of threads to: {}", this.numThreads);
    }

    /**
     * Gets an exception message
     * @param previousExceptionMessage the previous exception message that we wish to append to
     * @param metacardsIdsInError the metacard IDs that processed in error
     * @param operation the operation being performed
     * @return the updated exception message
     */
    private String getExceptionMessage(String responseType, String previousExceptionMessage, List<String> metacardsIdsInError, OPERATION operation) {
        StringBuilder builder = new StringBuilder();

        if (StringUtils.isNotBlank(previousExceptionMessage)) {
            builder.append(previousExceptionMessage);
            builder.append(" ");
        }

        builder.append("Error processing ");
        builder.append(responseType);
        builder.append(". ");

        switch (operation) {
        case CREATE:
            builder.append("Unable to backup metacard(s) [");
            builder.append(StringUtils.join(metacardsIdsInError, ","));
            builder.append("]. ");
            break;
        case DELETE:
            builder.append("Unable to delete metacard(s) [");
            builder.append(StringUtils.join(metacardsIdsInError, ","));
            builder.append("] from backup. ");
            break;
        default:
            // Unsupported operation
            break;
        }
        return builder.toString();
    }

    private File getBackupFile(Metacard metacard, File backupDirectory, int subDirectoryLevels) throws IOException {

        String metacardId = metacard.getId();
        File parent = backupDirectory;
        int levels = subDirectoryLevels;

        if (subDirectoryLevels < 0) {
            levels = 0;
        } else if (metacardId.length() == 1 || metacardId.length() < subDirectoryLevels * 2) {
            levels = (int) Math.floor(metacardId.length() / 2);
        }

        for (int i = 0; i < levels; i++) {
            parent = new File(parent, metacardId.substring(i * 2, i * 2 + 2));
            FileUtils.forceMkdir(parent);
        }

        LOGGER.debug("Backup directory for metacard  [{}] is [{}].", metacard.getId(), parent.getAbsolutePath());
        return new File(parent, metacardId);
    }
}
