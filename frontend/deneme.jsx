import { useState } from 'react'

function deneme() {
  const [begeniSayisi, setBegeniSayisi] = useState(0)
  const [begenildiMi, setBegenildiMi] = useState(false)

  function begenTikla() {
    // buraya senin yazman lazım
  }

  return (
    <div>
      <p>❤️ {begeniSayisi} beğeni</p>
      <button onClick={begenTikla}>
        {/* buraya "Beğen" ya da "Beğenildi" yazması lazım */}
      </button>
    </div>
  )
}

export default deneme
