import React from "react";

const JobsIcon = (props: React.SVGProps<SVGSVGElement>) => {
  return (
    <svg {...props} viewBox="0 0 32 32" fill="none">
      <path
        d="M16 4C9.37258 4 4 9.37258 4 16C4 22.6274 9.37258 28 16 28C22.6274 28 28 22.6274 28 16C28 9.37258 22.6274 4 16 4ZM16 6.5C21.2467 6.5 25.5 10.7533 25.5 16C25.5 21.2467 21.2467 25.5 16 25.5C10.7533 25.5 6.5 21.2467 6.5 16C6.5 10.7533 10.7533 6.5 16 6.5Z"
        fill="#0569EA"
      />
      <path
        d="M15 10V16.414L19.293 20.707L20.707 19.293L17 15.586V10H15Z"
        fill="#0569EA"
      />
    </svg>
  );
};

export { JobsIcon };
