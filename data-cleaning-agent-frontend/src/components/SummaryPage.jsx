import React from "react";
import styled from "styled-components";
import { FaFileDownload } from "react-icons/fa";

const Wrapper = styled.div`
  min-height: 100vh;
  background: linear-gradient(135deg, #181e41 40%, #fc466b 100%);
  padding: 2.5rem 0;
`;

const SummaryBox = styled.div`
  background: rgba(39, 47, 89, 0.75);
  border-radius: 28px;
  padding: 2.5rem 2.2rem;
  box-shadow: 0 6px 44px 0 #fc466b33;
  margin: 2.5rem auto;
  max-width: 700px;
  text-align: center;
  border: 1px solid #383d5a;
`;

const SectionTitle = styled.h2`
  font-weight: 800;
  font-size: 2.1rem;
  letter-spacing: 0.01em;
  margin-bottom: 1.2rem;
  color: #ffe082;
  display: flex;
  align-items: center;
  gap: 1.1rem;
  justify-content: center;
`;

const Details = styled.div`
  margin-top: 1.4rem;
  color: #fff;
  font-size: 1.13rem;
  text-align: left;
  ul {
    margin: 1rem 0;
    padding-left: 1.5rem;
  }
  li {
    margin: 0.5rem 0;
  }
`;

const StatsBox = styled.div`
  background: #3f5efb20;
  border-radius: 12px;
  padding: 1rem;
  margin: 1.5rem 0;
  text-align: left;
  h4 {
    color: #3f5efb;
    margin: 0 0 0.5rem 0;
    font-weight: bold;
  }
  p {
    color: #fff;
    margin: 0.3rem 0;
    font-size: 1rem;
  }
`;

const DownloadBtn = styled.a`
  display: inline-block;
  background: linear-gradient(90deg, #fc466b, #3f5efb 100%);
  border: none;
  padding: 1.1rem 2.8rem;
  border-radius: 48px;
  font-size: 1.33rem;
  font-weight: bold;
  color: #ffe082;
  margin-top: 2.2rem;
  cursor: pointer;
  box-shadow: 0 6px 22px #3f5efb33;
  text-decoration: none;
  transition: background 0.3s, color 0.3s;
  &:hover {
    background: linear-gradient(90deg, #ffe082, #fc466b 100%);
    color: #3f5efb;
  }
`;

const RestartBtn = styled.button`
  background: linear-gradient(90deg, #444, #888);
  border: none;
  padding: 0.8rem 1.8rem;
  border-radius: 32px;
  font-size: 1.08rem;
  font-weight: bold;
  color: #fff;
  margin-left: 1.4rem;
  cursor: pointer;
  box-shadow: 0 2px 10px #66666633;
  &:hover {
    background: linear-gradient(90deg, #888, #aaa);
  }
`;

export default function SummaryPage({ summary, data, setStep }) {
  // Calculate processing stats
  const originalMissing = 2; // Demo: originally had 2 missing values
  const currentMissing = data ? data.reduce((count, row) => 
    count + row.filter(cell => cell === "" || cell === null || cell === undefined).length, 0
  ) : 0;
  const manuallyFilled = originalMissing - currentMissing;

  return (
    <Wrapper>
      <SummaryBox>
        <SectionTitle>
          <FaFileDownload />
          Cleaning Summary
        </SectionTitle>
        
        <StatsBox>
          <h4>📈 Processing Statistics:</h4>
          <p>• Original missing values: {originalMissing}</p>
          <p>• Manually filled by user: {manuallyFilled}</p>
          <p>• Remaining missing values: {currentMissing}</p>
          <p>• Total rows processed: {data ? data.length : 0}</p>
        </StatsBox>
        
        <Details>
          <b>Steps performed:</b>
          <ul>
            {manuallyFilled > 0 && <li>✏️ User manually filled {manuallyFilled} missing values</li>}
            {currentMissing > 0 && <li>🧹 Dropped {currentMissing} remaining missing value cells</li>}
            <li>🔄 Dropped 1 duplicate row</li>
            <li>📊 Normalized numeric columns</li>
            <li>🏷️ One-hot encoded "City" column</li>
          </ul>
        </Details>
        
        <div style={{display: 'flex', justifyContent: 'center', alignItems: 'center', marginTop: '1.2rem'}}>
          <DownloadBtn href="#" download="cleaned_data.csv">
            Download Cleaned Data
          </DownloadBtn>
          <RestartBtn onClick={() => setStep(0)}>
            Start Over
          </RestartBtn>
        </div>
        
        <div style={{marginTop:'2.2rem', color:'#ffe082', fontWeight: 700, fontSize: "1.08rem"}}>
          🎉 <b>Your data is ready ! </b>
          {manuallyFilled > 0 && (
            <><br/><span style={{fontSize: '0.92rem', fontWeight: 400}}>
              Thanks for the manual touch - it makes the data even better! 🚀
            </span></>
          )}
        </div>
      </SummaryBox>
    </Wrapper>
  );
}