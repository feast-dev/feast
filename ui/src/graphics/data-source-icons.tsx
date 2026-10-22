import React from "react";

export const BigQueryIcon = (props: React.SVGProps<SVGSVGElement>) => (
  <svg {...props} viewBox="0 0 64 64" fill="none">
    <path
      d="M14.5 51.5l-2.3-2.3c-5.7-5.7-8.2-14-6.7-21.8 1.5-7.7 6.7-14.2 13.9-17.3 7.1-3.1 15.3-2.5 21.9 1.7l-2.5 3.3c-5.4-3.5-12.2-3.9-18-1.4-5.9 2.5-10.2 7.9-11.4 14.2-1.3 6.4.8 13.2 5.5 17.9l-0.4 5.7z"
      fill="#4285F4"
    />
    <path
      d="M51.5 49.7L45.8 50l0.3-5.6c4.7-4.7 6.8-11.5 5.5-17.9-1.3-6.4-5.5-11.7-11.4-14.2l1.1-3.7c7.2 3.1 12.4 9.6 13.9 17.3 1.5 7.8-1 16.1-6.7 21.8l3 2z"
      fill="#4285F4"
    />
    <path
      d="M32 22c5.5 0 10 4.5 10 10s-4.5 10-10 10-10-4.5-10-10 4.5-10 10-10zm0 4c-3.3 0-6 2.7-6 6s2.7 6 6 6 6-2.7 6-6-2.7-6-6-6z"
      fill="#4285F4"
    />
    <path d="M32 34h12v4H32z" fill="#4285F4" />
  </svg>
);

export const SnowflakeIcon = (props: React.SVGProps<SVGSVGElement>) => (
  <svg {...props} viewBox="0 0 64 64" fill="none">
    <path
      d="M32 4v56M11.1 20l41.8 24M11.1 44l41.8-24"
      stroke="#29B5E8"
      strokeWidth="4"
      strokeLinecap="round"
    />
    <path
      d="M32 4l-5 5M32 4l5 5M32 60l-5-5M32 60l5-5M11.1 20l1.5 6.5M11.1 20l6.5-1.5M52.9 44l-1.5-6.5M52.9 44l-6.5 1.5M11.1 44l1.5-6.5M11.1 44l6.5 1.5M52.9 20l-1.5 6.5M52.9 20l-6.5-1.5"
      stroke="#29B5E8"
      strokeWidth="3"
      strokeLinecap="round"
    />
  </svg>
);

export const RedshiftIcon = (props: React.SVGProps<SVGSVGElement>) => (
  <svg {...props} viewBox="0 0 64 64" fill="none">
    <path d="M32 8L12 20v24l20 12 20-12V20L32 8z" fill="#205B97" />
    <path d="M32 8L12 20l20 12 20-12L32 8z" fill="#5294CF" />
    <path d="M32 32v24l20-12V20L32 32z" fill="#2E73B8" />
    <path d="M32 32L12 20v24l20 12V32z" fill="#205B97" />
  </svg>
);

export const KafkaIcon = (props: React.SVGProps<SVGSVGElement>) => (
  <svg {...props} viewBox="0 0 64 64" fill="none">
    <circle cx="32" cy="20" r="6" fill="#231F20" />
    <circle cx="20" cy="38" r="5" fill="#231F20" />
    <circle cx="44" cy="38" r="5" fill="#231F20" />
    <circle cx="32" cy="50" r="4" fill="#231F20" />
    <circle cx="18" cy="26" r="4" fill="#231F20" />
    <circle cx="46" cy="26" r="4" fill="#231F20" />
    <line x1="32" y1="26" x2="32" y2="46" stroke="#231F20" strokeWidth="2" />
    <line x1="27" y1="22" x2="22" y2="26" stroke="#231F20" strokeWidth="2" />
    <line x1="37" y1="22" x2="42" y2="26" stroke="#231F20" strokeWidth="2" />
    <line x1="20" y1="30" x2="21" y2="33" stroke="#231F20" strokeWidth="2" />
    <line x1="44" y1="30" x2="43" y2="33" stroke="#231F20" strokeWidth="2" />
    <line x1="24" y1="40" x2="28" y2="48" stroke="#231F20" strokeWidth="2" />
    <line x1="40" y1="40" x2="36" y2="48" stroke="#231F20" strokeWidth="2" />
  </svg>
);

export const SparkIcon = (props: React.SVGProps<SVGSVGElement>) => (
  <svg {...props} viewBox="0 0 64 64" fill="none">
    <path
      d="M32 6c-2 8-8 14-14 18 6 2 12 8 14 16 2-8 8-14 14-16-6-4-12-10-14-18z"
      fill="#E25A1C"
    />
    <path
      d="M18 36c-1.5 5-5 9-9 12 4 1.5 8 5.5 9 10 1.5-5 5-9 9-10-4-3-7.5-7-9-12z"
      fill="#E25A1C"
      opacity="0.7"
    />
    <path
      d="M46 36c-1 4-4 7-7 9 3 1 6 4.5 7 8 1-4 4-7 7-8-3-2-6-5-7-9z"
      fill="#E25A1C"
      opacity="0.5"
    />
  </svg>
);

export const FileIcon = (props: React.SVGProps<SVGSVGElement>) => (
  <svg {...props} viewBox="0 0 64 64" fill="none">
    <path
      d="M16 8h22l12 12v36a2 2 0 01-2 2H16a2 2 0 01-2-2V10a2 2 0 012-2z"
      fill="#4CAF50"
      opacity="0.9"
    />
    <path d="M38 8l12 12H40a2 2 0 01-2-2V8z" fill="#388E3C" />
    <rect
      x="20"
      y="28"
      width="24"
      height="2"
      rx="1"
      fill="white"
      opacity="0.7"
    />
    <rect
      x="20"
      y="34"
      width="20"
      height="2"
      rx="1"
      fill="white"
      opacity="0.7"
    />
    <rect
      x="20"
      y="40"
      width="22"
      height="2"
      rx="1"
      fill="white"
      opacity="0.7"
    />
    <rect
      x="20"
      y="46"
      width="16"
      height="2"
      rx="1"
      fill="white"
      opacity="0.7"
    />
  </svg>
);

export const RequestSourceIcon = (props: React.SVGProps<SVGSVGElement>) => (
  <svg {...props} viewBox="0 0 64 64" fill="none">
    <rect
      x="10"
      y="14"
      width="44"
      height="36"
      rx="4"
      fill="#7B61FF"
      opacity="0.9"
    />
    <path d="M20 28l6 4-6 4V28z" fill="white" />
    <rect
      x="30"
      y="30"
      width="14"
      height="4"
      rx="2"
      fill="white"
      opacity="0.7"
    />
    <rect
      x="14"
      y="18"
      width="8"
      height="3"
      rx="1.5"
      fill="white"
      opacity="0.5"
    />
    <circle cx="48" cy="19.5" r="2" fill="#4CAF50" />
  </svg>
);

export const PushSourceIcon = (props: React.SVGProps<SVGSVGElement>) => (
  <svg {...props} viewBox="0 0 64 64" fill="none">
    <rect
      x="14"
      y="16"
      width="36"
      height="32"
      rx="3"
      fill="#FF6B35"
      opacity="0.9"
    />
    <path
      d="M32 24v16M26 34l6 6 6-6"
      stroke="white"
      strokeWidth="3"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
    <rect x="22" y="12" width="20" height="4" rx="2" fill="#FF6B35" />
  </svg>
);

export const KinesisIcon = (props: React.SVGProps<SVGSVGElement>) => (
  <svg {...props} viewBox="0 0 64 64" fill="none">
    <path d="M16 12h6l10 20-10 20h-6l10-20L16 12z" fill="#FF9900" />
    <path
      d="M28 12h6l10 20-10 20h-6l10-20L28 12z"
      fill="#FF9900"
      opacity="0.7"
    />
    <path
      d="M40 12h6l10 20-10 20h-6l10-20L40 12z"
      fill="#FF9900"
      opacity="0.5"
    />
  </svg>
);

export const TrinoIcon = (props: React.SVGProps<SVGSVGElement>) => (
  <svg {...props} viewBox="0 0 64 64" fill="none">
    <circle cx="32" cy="32" r="22" fill="#DD00A1" opacity="0.1" />
    <path
      d="M32 10c-12.15 0-22 9.85-22 22s9.85 22 22 22 22-9.85 22-22-9.85-22-22-22zm0 4c9.94 0 18 8.06 18 18s-8.06 18-18 18-18-8.06-18-18 8.06-18 18-18z"
      fill="#DD00A1"
    />
    <path d="M26 26h4v12h-4zM34 22h4v16h-4z" fill="#DD00A1" />
    <circle cx="28" cy="22" r="2.5" fill="#DD00A1" />
    <circle cx="36" cy="42" r="2.5" fill="#DD00A1" />
  </svg>
);

export const AthenaIcon = (props: React.SVGProps<SVGSVGElement>) => (
  <svg {...props} viewBox="0 0 64 64" fill="none">
    <path
      d="M32 8L14 56h6l4-12h16l4 12h6L32 8zm0 12l6 20H26l6-20z"
      fill="#8C4FFF"
    />
    <rect
      x="20"
      y="48"
      width="24"
      height="3"
      rx="1.5"
      fill="#8C4FFF"
      opacity="0.5"
    />
  </svg>
);

export const CustomSourceIcon = (props: React.SVGProps<SVGSVGElement>) => (
  <svg {...props} viewBox="0 0 64 64" fill="none">
    <rect
      x="12"
      y="12"
      width="40"
      height="40"
      rx="6"
      fill="#607D8B"
      opacity="0.15"
    />
    <path
      d="M24 28l-6 4 6 4M40 28l6 4-6 4M35 22l-6 20"
      stroke="#607D8B"
      strokeWidth="3"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  </svg>
);

export const RayIcon = (props: React.SVGProps<SVGSVGElement>) => (
  <svg {...props} viewBox="0 0 64 64" fill="none">
    <circle cx="32" cy="32" r="20" fill="#00A2E8" opacity="0.12" />
    <path
      d="M32 12v40M12 32h40M18 18l28 28M46 18L18 46"
      stroke="#00A2E8"
      strokeWidth="3"
      strokeLinecap="round"
    />
    <circle cx="32" cy="32" r="5" fill="#00A2E8" />
  </svg>
);

export const PostgresIcon = (props: React.SVGProps<SVGSVGElement>) => (
  <svg {...props} viewBox="0 0 64 64" fill="none">
    <path
      d="M32 8C20 8 14 14 14 22c0 6 4 10 4 14v8c0 6 6 12 14 12s14-6 14-12v-8c0-4 4-8 4-14 0-8-6-14-18-14z"
      fill="#336791"
      opacity="0.9"
    />
    <ellipse cx="32" cy="22" rx="12" ry="8" fill="#336791" />
    <ellipse cx="32" cy="22" rx="10" ry="6" fill="white" opacity="0.3" />
    <path
      d="M26 30v12c0 4 3 7 6 7s6-3 6-7V30"
      stroke="white"
      strokeWidth="2"
      opacity="0.5"
    />
  </svg>
);

export const MongoDBIcon = (props: React.SVGProps<SVGSVGElement>) => (
  <svg {...props} viewBox="0 0 64 64" fill="none">
    <path
      d="M32 6c-1.5 8-4 14-4 22 0 10 2 18 4 30 2-12 4-20 4-30 0-8-2.5-14-4-22z"
      fill="#00ED64"
    />
    <path
      d="M32 6c-6 10-16 16-16 28 0 10 8 18 16 24"
      fill="#00684A"
      opacity="0.6"
    />
    <path
      d="M32 6c6 10 16 16 16 28 0 10-8 18-16 24"
      fill="#00ED64"
      opacity="0.6"
    />
  </svg>
);

export const SqlServerIcon = (props: React.SVGProps<SVGSVGElement>) => (
  <svg {...props} viewBox="0 0 64 64" fill="none">
    <rect
      x="12"
      y="16"
      width="40"
      height="32"
      rx="4"
      fill="#CC2927"
      opacity="0.9"
    />
    <path d="M20 28h8v2h-8zM20 34h12v2H20z" fill="white" opacity="0.7" />
    <ellipse cx="42" cy="26" rx="6" ry="4" fill="white" opacity="0.5" />
    <path
      d="M36 26v12c0 2 3 4 6 4s6-2 6-4V26"
      stroke="white"
      strokeWidth="1.5"
      opacity="0.5"
    />
  </svg>
);

export const OracleIcon = (props: React.SVGProps<SVGSVGElement>) => (
  <svg {...props} viewBox="0 0 64 64" fill="none">
    <rect
      x="10"
      y="20"
      width="44"
      height="24"
      rx="12"
      fill="#F80000"
      opacity="0.9"
    />
    <rect
      x="16"
      y="26"
      width="32"
      height="12"
      rx="6"
      fill="white"
      opacity="0.3"
    />
    <text
      x="32"
      y="36"
      textAnchor="middle"
      fill="white"
      fontSize="10"
      fontWeight="bold"
    >
      ORA
    </text>
  </svg>
);

export const CouchbaseIcon = (props: React.SVGProps<SVGSVGElement>) => (
  <svg {...props} viewBox="0 0 64 64" fill="none">
    <circle cx="32" cy="32" r="22" fill="#EA2328" opacity="0.15" />
    <circle cx="32" cy="32" r="16" fill="#EA2328" opacity="0.3" />
    <circle cx="32" cy="32" r="8" fill="#EA2328" />
    <path d="M32 24v16M24 32h16" stroke="white" strokeWidth="2" />
  </svg>
);

export const ClickHouseIcon = (props: React.SVGProps<SVGSVGElement>) => (
  <svg {...props} viewBox="0 0 64 64" fill="none">
    <rect x="14" y="12" width="6" height="40" fill="#FFCC00" />
    <rect x="22" y="12" width="6" height="40" fill="#FFCC00" />
    <rect x="30" y="12" width="6" height="40" fill="#FFCC00" />
    <rect x="38" y="12" width="6" height="40" fill="#FFCC00" />
    <rect x="46" y="20" width="6" height="24" rx="3" fill="#FFCC00" />
  </svg>
);
