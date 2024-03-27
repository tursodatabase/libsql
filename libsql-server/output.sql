PRAGMA foreign_keys = ON;
create table `strapi_migrations` (`id` integer not null primary key autoincrement, `name` varchar(255), `time` datetime)
create table `strapi_database_schema` (`id` integer not null primary key autoincrement, `schema` json, `time` datetime, `hash` varchar(255))
PRAGMA foreign_key_list (`strapi_migrations`);
PRAGMA foreign_key_list (`strapi_database_schema`);
PRAGMA foreign_keys = off;
BEGIN;
create table `strapi_core_store_settings` (`id` integer not null primary key autoincrement, `key` varchar(255) null, `value` text null, `type` varchar(255) null, `environment` varchar(255) null, `tag` varchar(255) null)
COMMIT;
BEGIN;
create table `strapi_webhooks` (`id` integer not null primary key autoincrement, `name` varchar(255) null, `url` text null, `headers` json null, `events` json null, `enabled` boolean null)
COMMIT;
BEGIN;
create table `admin_permissions` (`id` integer not null primary key autoincrement, `action` varchar(255) null, `action_parameters` json null, `subject` varchar(255) null, `properties` json null, `conditions` json null, `created_at` datetime null, `updated_at` datetime null, `created_by_id` integer null, `updated_by_id` integer null, constraint `admin_permissions_created_by_id_fk` foreign key(`created_by_id`) references `admin_users`(`id`) on delete SET NULL, constraint `admin_permissions_updated_by_id_fk` foreign key(`updated_by_id`) references `admin_users`(`id`) on delete SET NULL)
CREATE INDEX `admin_permissions_created_by_id_fk` ON `admin_permissions` (`created_by_id`);
CREATE INDEX `admin_permissions_updated_by_id_fk` ON `admin_permissions` (`updated_by_id`);
COMMIT;
BEGIN;
create table `admin_users` (`id` integer not null primary key autoincrement, `firstname` varchar(255) null, `lastname` varchar(255) null, `username` varchar(255) null, `email` varchar(255) null, `password` varchar(255) null, `reset_password_token` varchar(255) null, `registration_token` varchar(255) null, `is_active` boolean null, `blocked` boolean null, `prefered_language` varchar(255) null, `created_at` datetime null, `updated_at` datetime null, `created_by_id` integer null, `updated_by_id` integer null, constraint `admin_users_created_by_id_fk` foreign key(`created_by_id`) references `admin_users`(`id`) on delete SET NULL, constraint `admin_users_updated_by_id_fk` foreign key(`updated_by_id`) references `admin_users`(`id`) on delete SET NULL)
CREATE INDEX `admin_users_created_by_id_fk` ON `admin_users` (`created_by_id`);
CREATE INDEX `admin_users_updated_by_id_fk` ON `admin_users` (`updated_by_id`);
COMMIT;
BEGIN;
create table `admin_roles` (`id` integer not null primary key autoincrement, `name` varchar(255) null, `code` varchar(255) null, `description` varchar(255) null, `created_at` datetime null, `updated_at` datetime null, `created_by_id` integer null, `updated_by_id` integer null, constraint `admin_roles_created_by_id_fk` foreign key(`created_by_id`) references `admin_users`(`id`) on delete SET NULL, constraint `admin_roles_updated_by_id_fk` foreign key(`updated_by_id`) references `admin_users`(`id`) on delete SET NULL)
CREATE INDEX `admin_roles_created_by_id_fk` ON `admin_roles` (`created_by_id`);
CREATE INDEX `admin_roles_updated_by_id_fk` ON `admin_roles` (`updated_by_id`);
COMMIT;
BEGIN;
create table `strapi_api_tokens` (`id` integer not null primary key autoincrement, `name` varchar(255) null, `description` varchar(255) null, `type` varchar(255) null, `access_key` varchar(255) null, `last_used_at` datetime null, `expires_at` datetime null, `lifespan` bigint null, `created_at` datetime null, `updated_at` datetime null, `created_by_id` integer null, `updated_by_id` integer null, constraint `strapi_api_tokens_created_by_id_fk` foreign key(`created_by_id`) references `admin_users`(`id`) on delete SET NULL, constraint `strapi_api_tokens_updated_by_id_fk` foreign key(`updated_by_id`) references `admin_users`(`id`) on delete SET NULL)
CREATE INDEX `strapi_api_tokens_created_by_id_fk` ON `strapi_api_tokens` (`created_by_id`);
CREATE INDEX `strapi_api_tokens_updated_by_id_fk` ON `strapi_api_tokens` (`updated_by_id`);
COMMIT;
BEGIN;
create table `strapi_api_token_permissions` (`id` integer not null primary key autoincrement, `action` varchar(255) null, `created_at` datetime null, `updated_at` datetime null, `created_by_id` integer null, `updated_by_id` integer null, constraint `strapi_api_token_permissions_created_by_id_fk` foreign key(`created_by_id`) references `admin_users`(`id`) on delete SET NULL, constraint `strapi_api_token_permissions_updated_by_id_fk` foreign key(`updated_by_id`) references `admin_users`(`id`) on delete SET NULL)
CREATE INDEX `strapi_api_token_permissions_created_by_id_fk` ON `strapi_api_token_permissions` (`created_by_id`);
CREATE INDEX `strapi_api_token_permissions_updated_by_id_fk` ON `strapi_api_token_permissions` (`updated_by_id`);
COMMIT;
BEGIN;
create table `strapi_transfer_tokens` (`id` integer not null primary key autoincrement, `name` varchar(255) null, `description` varchar(255) null, `access_key` varchar(255) null, `last_used_at` datetime null, `expires_at` datetime null, `lifespan` bigint null, `created_at` datetime null, `updated_at` datetime null, `created_by_id` integer null, `updated_by_id` integer null, constraint `strapi_transfer_tokens_created_by_id_fk` foreign key(`created_by_id`) references `admin_users`(`id`) on delete SET NULL, constraint `strapi_transfer_tokens_updated_by_id_fk` foreign key(`updated_by_id`) references `admin_users`(`id`) on delete SET NULL)
CREATE INDEX `strapi_transfer_tokens_created_by_id_fk` ON `strapi_transfer_tokens` (`created_by_id`);
CREATE INDEX `strapi_transfer_tokens_updated_by_id_fk` ON `strapi_transfer_tokens` (`updated_by_id`);
COMMIT;
BEGIN;
create table `strapi_transfer_token_permissions` (`id` integer not null primary key autoincrement, `action` varchar(255) null, `created_at` datetime null, `updated_at` datetime null, `created_by_id` integer null, `updated_by_id` integer null, constraint `strapi_transfer_token_permissions_created_by_id_fk` foreign key(`created_by_id`) references `admin_users`(`id`) on delete SET NULL, constraint `strapi_transfer_token_permissions_updated_by_id_fk` foreign key(`updated_by_id`) references `admin_users`(`id`) on delete SET NULL)
CREATE INDEX `strapi_transfer_token_permissions_created_by_id_fk` ON `strapi_transfer_token_permissions` (`created_by_id`);
CREATE INDEX `strapi_transfer_token_permissions_updated_by_id_fk` ON `strapi_transfer_token_permissions` (`updated_by_id`);
COMMIT;
BEGIN;
create table `files` (`id` integer not null primary key autoincrement, `name` varchar(255) null, `alternative_text` varchar(255) null, `caption` varchar(255) null, `width` integer null, `height` integer null, `formats` json null, `hash` varchar(255) null, `ext` varchar(255) null, `mime` varchar(255) null, `size` float null, `url` varchar(255) null, `preview_url` varchar(255) null, `provider` varchar(255) null, `provider_metadata` json null, `folder_path` varchar(255) null, `created_at` datetime null, `updated_at` datetime null, `created_by_id` integer null, `updated_by_id` integer null, constraint `files_created_by_id_fk` foreign key(`created_by_id`) references `admin_users`(`id`) on delete SET NULL, constraint `files_updated_by_id_fk` foreign key(`updated_by_id`) references `admin_users`(`id`) on delete SET NULL)
CREATE INDEX `upload_files_folder_path_index` ON `files` (`folder_path`);
CREATE INDEX `upload_files_created_at_index` ON `files` (`created_at`);
CREATE INDEX `upload_files_updated_at_index` ON `files` (`updated_at`);
CREATE INDEX `upload_files_name_index` ON `files` (`name`);
CREATE INDEX `upload_files_size_index` ON `files` (`size`);
CREATE INDEX `upload_files_ext_index` ON `files` (`ext`);
CREATE INDEX `files_created_by_id_fk` ON `files` (`created_by_id`);
CREATE INDEX `files_updated_by_id_fk` ON `files` (`updated_by_id`);
COMMIT;
BEGIN;
create table `upload_folders` (`id` integer not null primary key autoincrement, `name` varchar(255) null, `path_id` integer null, `path` varchar(255) null, `created_at` datetime null, `updated_at` datetime null, `created_by_id` integer null, `updated_by_id` integer null, constraint `upload_folders_created_by_id_fk` foreign key(`created_by_id`) references `admin_users`(`id`) on delete SET NULL, constraint `upload_folders_updated_by_id_fk` foreign key(`updated_by_id`) references `admin_users`(`id`) on delete SET NULL)
CREATE UNIQUE INDEX `upload_folders_path_id_index` ON `upload_folders` (`path_id`);
CREATE UNIQUE INDEX `upload_folders_path_index` ON `upload_folders` (`path`);
CREATE INDEX `upload_folders_created_by_id_fk` ON `upload_folders` (`created_by_id`);
CREATE INDEX `upload_folders_updated_by_id_fk` ON `upload_folders` (`updated_by_id`);
COMMIT;
BEGIN;
create table `strapi_releases` (`id` integer not null primary key autoincrement, `name` varchar(255) null, `released_at` datetime null, `scheduled_at` datetime null, `timezone` varchar(255) null, `status` varchar(255) null, `created_at` datetime null, `updated_at` datetime null, `created_by_id` integer null, `updated_by_id` integer null, constraint `strapi_releases_created_by_id_fk` foreign key(`created_by_id`) references `admin_users`(`id`) on delete SET NULL, constraint `strapi_releases_updated_by_id_fk` foreign key(`updated_by_id`) references `admin_users`(`id`) on delete SET NULL)
CREATE INDEX `strapi_releases_created_by_id_fk` ON `strapi_releases` (`created_by_id`);
CREATE INDEX `strapi_releases_updated_by_id_fk` ON `strapi_releases` (`updated_by_id`);
COMMIT;
BEGIN;
create table `strapi_release_actions` (`id` integer not null primary key autoincrement, `type` varchar(255) null, `target_id` integer null, `target_type` varchar(255) null, `content_type` varchar(255) null, `locale` varchar(255) null, `is_entry_valid` boolean null, `created_at` datetime null, `updated_at` datetime null, `created_by_id` integer null, `updated_by_id` integer null, constraint `strapi_release_actions_created_by_id_fk` foreign key(`created_by_id`) references `admin_users`(`id`) on delete SET NULL, constraint `strapi_release_actions_updated_by_id_fk` foreign key(`updated_by_id`) references `admin_users`(`id`) on delete SET NULL)
CREATE INDEX `strapi_release_actions_created_by_id_fk` ON `strapi_release_actions` (`created_by_id`);
CREATE INDEX `strapi_release_actions_updated_by_id_fk` ON `strapi_release_actions` (`updated_by_id`);
COMMIT;
BEGIN;
create table `up_permissions` (`id` integer not null primary key autoincrement, `action` varchar(255) null, `created_at` datetime null, `updated_at` datetime null, `created_by_id` integer null, `updated_by_id` integer null, constraint `up_permissions_created_by_id_fk` foreign key(`created_by_id`) references `admin_users`(`id`) on delete SET NULL, constraint `up_permissions_updated_by_id_fk` foreign key(`updated_by_id`) references `admin_users`(`id`) on delete SET NULL)
CREATE INDEX `up_permissions_created_by_id_fk` ON `up_permissions` (`created_by_id`);
CREATE INDEX `up_permissions_updated_by_id_fk` ON `up_permissions` (`updated_by_id`);
COMMIT;
BEGIN;
create table `up_roles` (`id` integer not null primary key autoincrement, `name` varchar(255) null, `description` varchar(255) null, `type` varchar(255) null, `created_at` datetime null, `updated_at` datetime null, `created_by_id` integer null, `updated_by_id` integer null, constraint `up_roles_created_by_id_fk` foreign key(`created_by_id`) references `admin_users`(`id`) on delete SET NULL, constraint `up_roles_updated_by_id_fk` foreign key(`updated_by_id`) references `admin_users`(`id`) on delete SET NULL)
CREATE INDEX `up_roles_created_by_id_fk` ON `up_roles` (`created_by_id`);
CREATE INDEX `up_roles_updated_by_id_fk` ON `up_roles` (`updated_by_id`);
COMMIT;
BEGIN;
create table `up_users` (`id` integer not null primary key autoincrement, `username` varchar(255) null, `email` varchar(255) null, `provider` varchar(255) null, `password` varchar(255) null, `reset_password_token` varchar(255) null, `confirmation_token` varchar(255) null, `confirmed` boolean null, `blocked` boolean null, `created_at` datetime null, `updated_at` datetime null, `created_by_id` integer null, `updated_by_id` integer null, constraint `up_users_created_by_id_fk` foreign key(`created_by_id`) references `admin_users`(`id`) on delete SET NULL, constraint `up_users_updated_by_id_fk` foreign key(`updated_by_id`) references `admin_users`(`id`) on delete SET NULL)
CREATE INDEX `up_users_created_by_id_fk` ON `up_users` (`created_by_id`);
CREATE INDEX `up_users_updated_by_id_fk` ON `up_users` (`updated_by_id`);
COMMIT;
BEGIN;
create table `i18n_locale` (`id` integer not null primary key autoincrement, `name` varchar(255) null, `code` varchar(255) null, `created_at` datetime null, `updated_at` datetime null, `created_by_id` integer null, `updated_by_id` integer null, constraint `i18n_locale_created_by_id_fk` foreign key(`created_by_id`) references `admin_users`(`id`) on delete SET NULL, constraint `i18n_locale_updated_by_id_fk` foreign key(`updated_by_id`) references `admin_users`(`id`) on delete SET NULL)
CREATE INDEX `i18n_locale_created_by_id_fk` ON `i18n_locale` (`created_by_id`);
CREATE INDEX `i18n_locale_updated_by_id_fk` ON `i18n_locale` (`updated_by_id`);
COMMIT;
BEGIN;
create table `admin_permissions_role_links` (`id` integer not null primary key autoincrement, `permission_id` integer null, `role_id` integer null, `permission_order` float null, constraint `admin_permissions_role_links_fk` foreign key(`permission_id`) references `admin_permissions`(`id`) on delete CASCADE, constraint `admin_permissions_role_links_inv_fk` foreign key(`role_id`) references `admin_roles`(`id`) on delete CASCADE)
CREATE INDEX `admin_permissions_role_links_fk` ON `admin_permissions_role_links` (`permission_id`);
CREATE INDEX `admin_permissions_role_links_inv_fk` ON `admin_permissions_role_links` (`role_id`);
CREATE UNIQUE INDEX `admin_permissions_role_links_unique` ON `admin_permissions_role_links` (`permission_id`, `role_id`);
CREATE INDEX `admin_permissions_role_links_order_inv_fk` ON `admin_permissions_role_links` (`permission_order`);
COMMIT;
BEGIN;
create table `admin_users_roles_links` (`id` integer not null primary key autoincrement, `user_id` integer null, `role_id` integer null, `role_order` float null, `user_order` float null, constraint `admin_users_roles_links_fk` foreign key(`user_id`) references `admin_users`(`id`) on delete CASCADE, constraint `admin_users_roles_links_inv_fk` foreign key(`role_id`) references `admin_roles`(`id`) on delete CASCADE)
CREATE INDEX `admin_users_roles_links_fk` ON `admin_users_roles_links` (`user_id`);
CREATE INDEX `admin_users_roles_links_inv_fk` ON `admin_users_roles_links` (`role_id`);
CREATE UNIQUE INDEX `admin_users_roles_links_unique` ON `admin_users_roles_links` (`user_id`, `role_id`);
CREATE INDEX `admin_users_roles_links_order_fk` ON `admin_users_roles_links` (`role_order`);
CREATE INDEX `admin_users_roles_links_order_inv_fk` ON `admin_users_roles_links` (`user_order`);
COMMIT;
BEGIN;
create table `strapi_api_token_permissions_token_links` (`id` integer not null primary key autoincrement, `api_token_permission_id` integer null, `api_token_id` integer null, `api_token_permission_order` float null, constraint `strapi_api_token_permissions_token_links_fk` foreign key(`api_token_permission_id`) references `strapi_api_token_permissions`(`id`) on delete CASCADE, constraint `strapi_api_token_permissions_token_links_inv_fk` foreign key(`api_token_id`) references `strapi_api_tokens`(`id`) on delete CASCADE)
CREATE INDEX `strapi_api_token_permissions_token_links_fk` ON `strapi_api_token_permissions_token_links` (`api_token_permission_id`);
CREATE INDEX `strapi_api_token_permissions_token_links_inv_fk` ON `strapi_api_token_permissions_token_links` (`api_token_id`);
CREATE UNIQUE INDEX `strapi_api_token_permissions_token_links_unique` ON `strapi_api_token_permissions_token_links` (`api_token_permission_id`, `api_token_id`);
CREATE INDEX `strapi_api_token_permissions_token_links_order_inv_fk` ON `strapi_api_token_permissions_token_links` (`api_token_permission_order`);
COMMIT;
BEGIN;
create table `strapi_transfer_token_permissions_token_links` (`id` integer not null primary key autoincrement, `transfer_token_permission_id` integer null, `transfer_token_id` integer null, `transfer_token_permission_order` float null, constraint `strapi_transfer_token_permissions_token_links_fk` foreign key(`transfer_token_permission_id`) references `strapi_transfer_token_permissions`(`id`) on delete CASCADE, constraint `strapi_transfer_token_permissions_token_links_inv_fk` foreign key(`transfer_token_id`) references `strapi_transfer_tokens`(`id`) on delete CASCADE)
CREATE INDEX `strapi_transfer_token_permissions_token_links_fk` ON `strapi_transfer_token_permissions_token_links` (`transfer_token_permission_id`);
CREATE INDEX `strapi_transfer_token_permissions_token_links_inv_fk` ON `strapi_transfer_token_permissions_token_links` (`transfer_token_id`);
CREATE UNIQUE INDEX `strapi_transfer_token_permissions_token_links_unique` ON `strapi_transfer_token_permissions_token_links` (`transfer_token_permission_id`, `transfer_token_id`);
CREATE INDEX `strapi_transfer_token_permissions_token_links_order_inv_fk` ON `strapi_transfer_token_permissions_token_links` (`transfer_token_permission_order`);
COMMIT;
BEGIN;
create table `files_related_morphs` (`id` integer not null primary key autoincrement, `file_id` integer null, `related_id` integer null, `related_type` varchar(255) null, `field` varchar(255) null, `order` float null, constraint `files_related_morphs_fk` foreign key(`file_id`) references `files`(`id`) on delete CASCADE)
CREATE INDEX `files_related_morphs_fk` ON `files_related_morphs` (`file_id`);
CREATE INDEX `files_related_morphs_order_index` ON `files_related_morphs` (`order`);
CREATE INDEX `files_related_morphs_id_column_index` ON `files_related_morphs` (`related_id`);
COMMIT;
BEGIN;
create table `files_folder_links` (`id` integer not null primary key autoincrement, `file_id` integer null, `folder_id` integer null, `file_order` float null, constraint `files_folder_links_fk` foreign key(`file_id`) references `files`(`id`) on delete CASCADE, constraint `files_folder_links_inv_fk` foreign key(`folder_id`) references `upload_folders`(`id`) on delete CASCADE)
CREATE INDEX `files_folder_links_fk` ON `files_folder_links` (`file_id`);
CREATE INDEX `files_folder_links_inv_fk` ON `files_folder_links` (`folder_id`);
CREATE UNIQUE INDEX `files_folder_links_unique` ON `files_folder_links` (`file_id`, `folder_id`);
CREATE INDEX `files_folder_links_order_inv_fk` ON `files_folder_links` (`file_order`);
COMMIT;
BEGIN;
create table `upload_folders_parent_links` (`id` integer not null primary key autoincrement, `folder_id` integer null, `inv_folder_id` integer null, `folder_order` float null, constraint `upload_folders_parent_links_fk` foreign key(`folder_id`) references `upload_folders`(`id`) on delete CASCADE, constraint `upload_folders_parent_links_inv_fk` foreign key(`inv_folder_id`) references `upload_folders`(`id`) on delete CASCADE)
CREATE INDEX `upload_folders_parent_links_fk` ON `upload_folders_parent_links` (`folder_id`);
CREATE INDEX `upload_folders_parent_links_inv_fk` ON `upload_folders_parent_links` (`inv_folder_id`);
CREATE UNIQUE INDEX `upload_folders_parent_links_unique` ON `upload_folders_parent_links` (`folder_id`, `inv_folder_id`);
CREATE INDEX `upload_folders_parent_links_order_inv_fk` ON `upload_folders_parent_links` (`folder_order`);
COMMIT;
BEGIN;
create table `strapi_release_actions_release_links` (`id` integer not null primary key autoincrement, `release_action_id` integer null, `release_id` integer null, `release_action_order` float null, constraint `strapi_release_actions_release_links_fk` foreign key(`release_action_id`) references `strapi_release_actions`(`id`) on delete CASCADE, constraint `strapi_release_actions_release_links_inv_fk` foreign key(`release_id`) references `strapi_releases`(`id`) on delete CASCADE)
CREATE INDEX `strapi_release_actions_release_links_fk` ON `strapi_release_actions_release_links` (`release_action_id`);
CREATE INDEX `strapi_release_actions_release_links_inv_fk` ON `strapi_release_actions_release_links` (`release_id`);
CREATE UNIQUE INDEX `strapi_release_actions_release_links_unique` ON `strapi_release_actions_release_links` (`release_action_id`, `release_id`);
CREATE INDEX `strapi_release_actions_release_links_order_inv_fk` ON `strapi_release_actions_release_links` (`release_action_order`);
COMMIT;
BEGIN;
create table `up_permissions_role_links` (`id` integer not null primary key autoincrement, `permission_id` integer null, `role_id` integer null, `permission_order` float null, constraint `up_permissions_role_links_fk` foreign key(`permission_id`) references `up_permissions`(`id`) on delete CASCADE, constraint `up_permissions_role_links_inv_fk` foreign key(`role_id`) references `up_roles`(`id`) on delete CASCADE)
CREATE INDEX `up_permissions_role_links_fk` ON `up_permissions_role_links` (`permission_id`);
CREATE INDEX `up_permissions_role_links_inv_fk` ON `up_permissions_role_links` (`role_id`);
CREATE UNIQUE INDEX `up_permissions_role_links_unique` ON `up_permissions_role_links` (`permission_id`, `role_id`);
CREATE INDEX `up_permissions_role_links_order_inv_fk` ON `up_permissions_role_links` (`permission_order`);
COMMIT;
BEGIN;
create table `up_users_role_links` (`id` integer not null primary key autoincrement, `user_id` integer null, `role_id` integer null, `user_order` float null, constraint `up_users_role_links_fk` foreign key(`user_id`) references `up_users`(`id`) on delete CASCADE, constraint `up_users_role_links_inv_fk` foreign key(`role_id`) references `up_roles`(`id`) on delete CASCADE)
CREATE INDEX `up_users_role_links_fk` ON `up_users_role_links` (`user_id`);
CREATE INDEX `up_users_role_links_inv_fk` ON `up_users_role_links` (`role_id`);
CREATE UNIQUE INDEX `up_users_role_links_unique` ON `up_users_role_links` (`user_id`, `role_id`);
CREATE INDEX `up_users_role_links_order_inv_fk` ON `up_users_role_links` (`user_order`);
COMMIT;
BEGIN;
COMMIT;
BEGIN;
COMMIT;
BEGIN;
SAVEPOINT trx32;
SELECT type, sql FROM sqlite_master WHERE (type = 'table' OR (type = 'index' AND sql IS NOT NULL)) AND lower (tbl_name) = 'admin_permissions';
RELEASE trx32;
PRAGMA foreign_keys;
SAVEPOINT trx33;
CREATE TABLE `_knex_temp_alter079` (`id` integer PRIMARY KEY AUTOINCREMENT NOT NULL, `action` varchar(255) NULL, `action_parameters` json NULL, `subject` varchar(255) NULL, `properties` json NULL, `conditions` json NULL, `created_at` datetime NULL, `updated_at` datetime NULL, `created_by_id` integer NULL, `updated_by_id` integer NULL, CONSTRAINT `admin_permissions_created_by_id_fk` FOREIGN KEY (`created_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL, CONSTRAINT `admin_permissions_updated_by_id_fk` FOREIGN KEY (`updated_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL, CONSTRAINT `admin_permissions_created_by_id_fk` FOREIGN KEY (`created_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL)
INSERT INTO "_knex_temp_alter079" SELECT * FROM "admin_permissions";
DROP TABLE "admin_permissions";
ALTER TABLE "_knex_temp_alter079" RENAME TO "admin_permissions";
CREATE INDEX `admin_permissions_created_by_id_fk` ON `admin_permissions` (`created_by_id`);
CREATE INDEX `admin_permissions_updated_by_id_fk` ON `admin_permissions` (`updated_by_id`);
RELEASE trx33;
SAVEPOINT trx34;
SELECT type, sql FROM sqlite_master WHERE (type = 'table' OR (type = 'index' AND sql IS NOT NULL)) AND lower (tbl_name) = 'admin_permissions';
RELEASE trx34;
PRAGMA foreign_keys;
SAVEPOINT trx35;
CREATE TABLE `_knex_temp_alter454` (`id` integer PRIMARY KEY AUTOINCREMENT NOT NULL, `action` varchar(255) NULL, `action_parameters` json NULL, `subject` varchar(255) NULL, `properties` json NULL, `conditions` json NULL, `created_at` datetime NULL, `updated_at` datetime NULL, `created_by_id` integer NULL, `updated_by_id` integer NULL, CONSTRAINT `admin_permissions_created_by_id_fk` FOREIGN KEY (`created_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL, CONSTRAINT `admin_permissions_updated_by_id_fk` FOREIGN KEY (`updated_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL, CONSTRAINT `admin_permissions_created_by_id_fk` FOREIGN KEY (`created_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL, CONSTRAINT `admin_permissions_updated_by_id_fk` FOREIGN KEY (`updated_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL)
INSERT INTO "_knex_temp_alter454" SELECT * FROM "admin_permissions";
DROP TABLE "admin_permissions";
ALTER TABLE "_knex_temp_alter454" RENAME TO "admin_permissions";
CREATE INDEX `admin_permissions_created_by_id_fk` ON `admin_permissions` (`created_by_id`);
CREATE INDEX `admin_permissions_updated_by_id_fk` ON `admin_permissions` (`updated_by_id`);
RELEASE trx35;
COMMIT;
BEGIN;
SAVEPOINT trx37;
SELECT type, sql FROM sqlite_master WHERE (type = 'table' OR (type = 'index' AND sql IS NOT NULL)) AND lower (tbl_name) = 'admin_users';
RELEASE trx37;
PRAGMA foreign_keys;
SAVEPOINT trx38;
CREATE TABLE `_knex_temp_alter752` (`id` integer PRIMARY KEY AUTOINCREMENT NOT NULL, `firstname` varchar(255) NULL, `lastname` varchar(255) NULL, `username` varchar(255) NULL, `email` varchar(255) NULL, `password` varchar(255) NULL, `reset_password_token` varchar(255) NULL, `registration_token` varchar(255) NULL, `is_active` boolean NULL, `blocked` boolean NULL, `prefered_language` varchar(255) NULL, `created_at` datetime NULL, `updated_at` datetime NULL, `created_by_id` integer NULL, `updated_by_id` integer NULL, CONSTRAINT `admin_users_created_by_id_fk` FOREIGN KEY (`created_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL, CONSTRAINT `admin_users_updated_by_id_fk` FOREIGN KEY (`updated_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL, CONSTRAINT `admin_users_created_by_id_fk` FOREIGN KEY (`created_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL)
INSERT INTO "_knex_temp_alter752" SELECT * FROM "admin_users";
DROP TABLE "admin_users";
ALTER TABLE "_knex_temp_alter752" RENAME TO "admin_users";
CREATE INDEX `admin_users_created_by_id_fk` ON `admin_users` (`created_by_id`);
CREATE INDEX `admin_users_updated_by_id_fk` ON `admin_users` (`updated_by_id`);
RELEASE trx38;
SAVEPOINT trx39;
SELECT type, sql FROM sqlite_master WHERE (type = 'table' OR (type = 'index' AND sql IS NOT NULL)) AND lower (tbl_name) = 'admin_users';
RELEASE trx39;
PRAGMA foreign_keys;
SAVEPOINT trx40;
CREATE TABLE `_knex_temp_alter590` (`id` integer PRIMARY KEY AUTOINCREMENT NOT NULL, `firstname` varchar(255) NULL, `lastname` varchar(255) NULL, `username` varchar(255) NULL, `email` varchar(255) NULL, `password` varchar(255) NULL, `reset_password_token` varchar(255) NULL, `registration_token` varchar(255) NULL, `is_active` boolean NULL, `blocked` boolean NULL, `prefered_language` varchar(255) NULL, `created_at` datetime NULL, `updated_at` datetime NULL, `created_by_id` integer NULL, `updated_by_id` integer NULL, CONSTRAINT `admin_users_created_by_id_fk` FOREIGN KEY (`created_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL, CONSTRAINT `admin_users_updated_by_id_fk` FOREIGN KEY (`updated_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL, CONSTRAINT `admin_users_created_by_id_fk` FOREIGN KEY (`created_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL, CONSTRAINT `admin_users_updated_by_id_fk` FOREIGN KEY (`updated_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL)
INSERT INTO "_knex_temp_alter590" SELECT * FROM "admin_users";
DROP TABLE "admin_users";
ALTER TABLE "_knex_temp_alter590" RENAME TO "admin_users";
CREATE INDEX `admin_users_created_by_id_fk` ON `admin_users` (`created_by_id`);
CREATE INDEX `admin_users_updated_by_id_fk` ON `admin_users` (`updated_by_id`);
RELEASE trx40;
COMMIT;
BEGIN;
SAVEPOINT trx42;
SELECT type, sql FROM sqlite_master WHERE (type = 'table' OR (type = 'index' AND sql IS NOT NULL)) AND lower (tbl_name) = 'admin_roles';
RELEASE trx42;
PRAGMA foreign_keys;
SAVEPOINT trx43;
CREATE TABLE `_knex_temp_alter231` (`id` integer PRIMARY KEY AUTOINCREMENT NOT NULL, `name` varchar(255) NULL, `code` varchar(255) NULL, `description` varchar(255) NULL, `created_at` datetime NULL, `updated_at` datetime NULL, `created_by_id` integer NULL, `updated_by_id` integer NULL, CONSTRAINT `admin_roles_created_by_id_fk` FOREIGN KEY (`created_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL, CONSTRAINT `admin_roles_updated_by_id_fk` FOREIGN KEY (`updated_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL, CONSTRAINT `admin_roles_created_by_id_fk` FOREIGN KEY (`created_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL)
INSERT INTO "_knex_temp_alter231" SELECT * FROM "admin_roles";
DROP TABLE "admin_roles";
ALTER TABLE "_knex_temp_alter231" RENAME TO "admin_roles";
CREATE INDEX `admin_roles_created_by_id_fk` ON `admin_roles` (`created_by_id`);
CREATE INDEX `admin_roles_updated_by_id_fk` ON `admin_roles` (`updated_by_id`);
RELEASE trx43;
SAVEPOINT trx44;
SELECT type, sql FROM sqlite_master WHERE (type = 'table' OR (type = 'index' AND sql IS NOT NULL)) AND lower (tbl_name) = 'admin_roles';
RELEASE trx44;
PRAGMA foreign_keys;
SAVEPOINT trx45;
CREATE TABLE `_knex_temp_alter768` (`id` integer PRIMARY KEY AUTOINCREMENT NOT NULL, `name` varchar(255) NULL, `code` varchar(255) NULL, `description` varchar(255) NULL, `created_at` datetime NULL, `updated_at` datetime NULL, `created_by_id` integer NULL, `updated_by_id` integer NULL, CONSTRAINT `admin_roles_created_by_id_fk` FOREIGN KEY (`created_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL, CONSTRAINT `admin_roles_updated_by_id_fk` FOREIGN KEY (`updated_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL, CONSTRAINT `admin_roles_created_by_id_fk` FOREIGN KEY (`created_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL, CONSTRAINT `admin_roles_updated_by_id_fk` FOREIGN KEY (`updated_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL)
INSERT INTO "_knex_temp_alter768" SELECT * FROM "admin_roles";
DROP TABLE "admin_roles";
ALTER TABLE "_knex_temp_alter768" RENAME TO "admin_roles";
CREATE INDEX `admin_roles_created_by_id_fk` ON `admin_roles` (`created_by_id`);
CREATE INDEX `admin_roles_updated_by_id_fk` ON `admin_roles` (`updated_by_id`);
RELEASE trx45;
COMMIT;
BEGIN;
SAVEPOINT trx47;
SELECT type, sql FROM sqlite_master WHERE (type = 'table' OR (type = 'index' AND sql IS NOT NULL)) AND lower (tbl_name) = 'strapi_api_tokens';
RELEASE trx47;
PRAGMA foreign_keys;
SAVEPOINT trx48;
CREATE TABLE `_knex_temp_alter309` (`id` integer PRIMARY KEY AUTOINCREMENT NOT NULL, `name` varchar(255) NULL, `description` varchar(255) NULL, `type` varchar(255) NULL, `access_key` varchar(255) NULL, `last_used_at` datetime NULL, `expires_at` datetime NULL, `lifespan` bigint NULL, `created_at` datetime NULL, `updated_at` datetime NULL, `created_by_id` integer NULL, `updated_by_id` integer NULL, CONSTRAINT `strapi_api_tokens_created_by_id_fk` FOREIGN KEY (`created_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL, CONSTRAINT `strapi_api_tokens_updated_by_id_fk` FOREIGN KEY (`updated_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL, CONSTRAINT `strapi_api_tokens_created_by_id_fk` FOREIGN KEY (`created_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL)
INSERT INTO "_knex_temp_alter309" SELECT * FROM "strapi_api_tokens";
DROP TABLE "strapi_api_tokens";
ALTER TABLE "_knex_temp_alter309" RENAME TO "strapi_api_tokens";
CREATE INDEX `strapi_api_tokens_created_by_id_fk` ON `strapi_api_tokens` (`created_by_id`);
CREATE INDEX `strapi_api_tokens_updated_by_id_fk` ON `strapi_api_tokens` (`updated_by_id`);
RELEASE trx48;
SAVEPOINT trx49;
SELECT type, sql FROM sqlite_master WHERE (type = 'table' OR (type = 'index' AND sql IS NOT NULL)) AND lower (tbl_name) = 'strapi_api_tokens';
RELEASE trx49;
PRAGMA foreign_keys;
SAVEPOINT trx50;
CREATE TABLE `_knex_temp_alter407` (`id` integer PRIMARY KEY AUTOINCREMENT NOT NULL, `name` varchar(255) NULL, `description` varchar(255) NULL, `type` varchar(255) NULL, `access_key` varchar(255) NULL, `last_used_at` datetime NULL, `expires_at` datetime NULL, `lifespan` bigint NULL, `created_at` datetime NULL, `updated_at` datetime NULL, `created_by_id` integer NULL, `updated_by_id` integer NULL, CONSTRAINT `strapi_api_tokens_created_by_id_fk` FOREIGN KEY (`created_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL, CONSTRAINT `strapi_api_tokens_updated_by_id_fk` FOREIGN KEY (`updated_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL, CONSTRAINT `strapi_api_tokens_created_by_id_fk` FOREIGN KEY (`created_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL, CONSTRAINT `strapi_api_tokens_updated_by_id_fk` FOREIGN KEY (`updated_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL)
INSERT INTO "_knex_temp_alter407" SELECT * FROM "strapi_api_tokens";
DROP TABLE "strapi_api_tokens";
ALTER TABLE "_knex_temp_alter407" RENAME TO "strapi_api_tokens";
CREATE INDEX `strapi_api_tokens_created_by_id_fk` ON `strapi_api_tokens` (`created_by_id`);
CREATE INDEX `strapi_api_tokens_updated_by_id_fk` ON `strapi_api_tokens` (`updated_by_id`);
RELEASE trx50;
COMMIT;
BEGIN;
SAVEPOINT trx52;
SELECT type, sql FROM sqlite_master WHERE (type = 'table' OR (type = 'index' AND sql IS NOT NULL)) AND lower (tbl_name) = 'strapi_api_token_permissions';
RELEASE trx52;
PRAGMA foreign_keys;
SAVEPOINT trx53;
CREATE TABLE `_knex_temp_alter524` (`id` integer PRIMARY KEY AUTOINCREMENT NOT NULL, `action` varchar(255) NULL, `created_at` datetime NULL, `updated_at` datetime NULL, `created_by_id` integer NULL, `updated_by_id` integer NULL, CONSTRAINT `strapi_api_token_permissions_created_by_id_fk` FOREIGN KEY (`created_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL, CONSTRAINT `strapi_api_token_permissions_updated_by_id_fk` FOREIGN KEY (`updated_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL, CONSTRAINT `strapi_api_token_permissions_created_by_id_fk` FOREIGN KEY (`created_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL)
INSERT INTO "_knex_temp_alter524" SELECT * FROM "strapi_api_token_permissions";
DROP TABLE "strapi_api_token_permissions";
ALTER TABLE "_knex_temp_alter524" RENAME TO "strapi_api_token_permissions";
CREATE INDEX `strapi_api_token_permissions_created_by_id_fk` ON `strapi_api_token_permissions` (`created_by_id`);
CREATE INDEX `strapi_api_token_permissions_updated_by_id_fk` ON `strapi_api_token_permissions` (`updated_by_id`);
RELEASE trx53;
SAVEPOINT trx54;
SELECT type, sql FROM sqlite_master WHERE (type = 'table' OR (type = 'index' AND sql IS NOT NULL)) AND lower (tbl_name) = 'strapi_api_token_permissions';
RELEASE trx54;
PRAGMA foreign_keys;
SAVEPOINT trx55;
CREATE TABLE `_knex_temp_alter412` (`id` integer PRIMARY KEY AUTOINCREMENT NOT NULL, `action` varchar(255) NULL, `created_at` datetime NULL, `updated_at` datetime NULL, `created_by_id` integer NULL, `updated_by_id` integer NULL, CONSTRAINT `strapi_api_token_permissions_created_by_id_fk` FOREIGN KEY (`created_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL, CONSTRAINT `strapi_api_token_permissions_updated_by_id_fk` FOREIGN KEY (`updated_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL, CONSTRAINT `strapi_api_token_permissions_created_by_id_fk` FOREIGN KEY (`created_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL, CONSTRAINT `strapi_api_token_permissions_updated_by_id_fk` FOREIGN KEY (`updated_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL)
INSERT INTO "_knex_temp_alter412" SELECT * FROM "strapi_api_token_permissions";
DROP TABLE "strapi_api_token_permissions";
ALTER TABLE "_knex_temp_alter412" RENAME TO "strapi_api_token_permissions";
CREATE INDEX `strapi_api_token_permissions_created_by_id_fk` ON `strapi_api_token_permissions` (`created_by_id`);
CREATE INDEX `strapi_api_token_permissions_updated_by_id_fk` ON `strapi_api_token_permissions` (`updated_by_id`);
RELEASE trx55;
COMMIT;
BEGIN;
SAVEPOINT trx57;
SELECT type, sql FROM sqlite_master WHERE (type = 'table' OR (type = 'index' AND sql IS NOT NULL)) AND lower (tbl_name) = 'strapi_transfer_tokens';
RELEASE trx57;
PRAGMA foreign_keys;
SAVEPOINT trx58;
CREATE TABLE `_knex_temp_alter605` (`id` integer PRIMARY KEY AUTOINCREMENT NOT NULL, `name` varchar(255) NULL, `description` varchar(255) NULL, `access_key` varchar(255) NULL, `last_used_at` datetime NULL, `expires_at` datetime NULL, `lifespan` bigint NULL, `created_at` datetime NULL, `updated_at` datetime NULL, `created_by_id` integer NULL, `updated_by_id` integer NULL, CONSTRAINT `strapi_transfer_tokens_created_by_id_fk` FOREIGN KEY (`created_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL, CONSTRAINT `strapi_transfer_tokens_updated_by_id_fk` FOREIGN KEY (`updated_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL, CONSTRAINT `strapi_transfer_tokens_created_by_id_fk` FOREIGN KEY (`created_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL)
--INSERT INTO "_knex_temp_alter605" SELECT * FROM "strapi_transfer_tokens";
--DROP TABLE "strapi_transfer_tokens";
--ALTER TABLE "_knex_temp_alter605" RENAME TO "strapi_transfer_tokens";
--CREATE INDEX `strapi_transfer_tokens_created_by_id_fk` ON `strapi_transfer_tokens` (`created_by_id`);
--CREATE INDEX `strapi_transfer_tokens_updated_by_id_fk` ON `strapi_transfer_tokens` (`updated_by_id`);
--RELEASE trx58;
--SAVEPOINT trx59;
--SELECT type, sql FROM sqlite_master WHERE (type = 'table' OR (type = 'index' AND sql IS NOT NULL)) AND lower (tbl_name) = 'strapi_transfer_tokens';
--RELEASE trx59;
--PRAGMA foreign_keys;
--SAVEPOINT trx60;
--CREATE TABLE `_knex_temp_alter855` (`id` integer PRIMARY KEY AUTOINCREMENT NOT NULL, `name` varchar(255) NULL, `description` varchar(255) NULL, `access_key` varchar(255) NULL, `last_used_at` datetime NULL, `expires_at` datetime NULL, `lifespan` bigint NULL, `created_at` datetime NULL, `updated_at` datetime NULL, `created_by_id` integer NULL, `updated_by_id` integer NULL, CONSTRAINT `strapi_transfer_tokens_created_by_id_fk` FOREIGN KEY (`created_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL, CONSTRAINT `strapi_transfer_tokens_updated_by_id_fk` FOREIGN KEY (`updated_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL, CONSTRAINT `strapi_transfer_tokens_created_by_id_fk` FOREIGN KEY (`created_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL, CONSTRAINT `strapi_transfer_tokens_updated_by_id_fk` FOREIGN KEY (`updated_by_id`) REFERENCES `admin_users` (`id`) ON DELETE SET NULL)
--INSERT INTO "_knex_temp_alter855" SELECT * FROM "strapi_transfer_tokens";
--DROP TABLE "strapi_transfer_tokens";
--ALTER TABLE "_knex_temp_alter855" RENAME TO "strapi_transfer_tokens";
--CREATE INDEX `strapi_transfer_tokens_created_by_id_fk` ON `strapi_transfer_tokens` (`created_by_id`);
--CREATE INDEX `strapi_transfer_tokens_updated_by_id_fk` ON `strapi_transfer_tokens` (`updated_by_id`);
--RELEASE trx60;
COMMIT;
