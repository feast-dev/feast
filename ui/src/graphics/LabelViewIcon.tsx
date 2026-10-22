import React from "react";

const LabelViewIcon = (props: React.SVGProps<SVGSVGElement>) => {
  return (
    <svg
      {...props}
      viewBox="0 0 32 32"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
    >
      <path
        d="M4 7C4 5.34315 5.34315 4 7 4H17.5858C18.1162 4 18.6249 4.21071 19 4.58579L27.4142 13C27.7893 13.3751 28 13.8838 28 14.4142V25C28 26.6569 26.6569 28 25 28H7C5.34315 28 4 26.6569 4 25V7Z"
        stroke="#0569EA"
        strokeWidth="2.5"
        fill="none"
      />
      <circle cx="10" cy="10" r="2" fill="#0569EA" />
      <path
        d="M10 17H22M10 21H18"
        stroke="#0569EA"
        strokeWidth="2"
        strokeLinecap="round"
      />
    </svg>
  );
};

export { LabelViewIcon };
