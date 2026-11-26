/// <reference types="vite/client" />
interface ImportMetaEnv {
  readonly VITE_REPO_URL: string;
  readonly VITE_GIT_SHA: string;
  readonly VITE_ADMIN_TOKEN?: string;
}
