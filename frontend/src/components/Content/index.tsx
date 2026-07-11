import styles from "./Content.module.css";

export function Content({ children }: { children: React.ReactNode }) {
  return <div className={styles.content}>{children}</div>;
}
